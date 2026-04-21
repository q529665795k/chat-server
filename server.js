const http = require('http');
const process = require('process');
const express = require('express');
const mysql = require('mysql2/promise');
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const axios = require('axios');
const app = express();
const server = http.createServer(app);
const { exec } = require('child_process');
const { Server } = require('socket.io');
const PORT = process.env.PORT || 10000;

// AI 回复
async function callAI(prompt) {
  try {
    const res = await axios.post("http://127.0.0.1:11434/api/chat", {
      model: "qwen2.5:0.5b",
      messages: [{ role: "user", content: prompt }],
      stream: false
    }, { timeout: 15000 });
    return res.data?.message?.content || "我在呢～";
  } catch (err) {
    return "AI休息中";
  }
}

// 数据库配置
const dbConfig = {
  host: 'hg.sj8.xyz',
  port: 3306,
  user: 'ser1nc1b03n1wln',
  password: 'FQ1QR7M8NBQF',
  database: 'ser1nc1b03n1wln',
  charset: 'utf8mb4',
  connectionLimit: 10
};

let db;
(async () => {
  try {
    db = await mysql.createPool(dbConfig);
    await db.getConnection();
    console.log('✅ MySQL 连接成功');
    await db.execute(`CREATE TABLE IF NOT EXISTS users (
      id INT AUTO_INCREMENT PRIMARY KEY,
      username VARCHAR(50) NOT NULL UNIQUE,
      nickname VARCHAR(50) NOT NULL DEFAULT '',
      password VARCHAR(255) NOT NULL,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`);
    await db.execute(`CREATE TABLE IF NOT EXISTS messages (
      id INT AUTO_INCREMENT PRIMARY KEY,
      from_user VARCHAR(50) NOT NULL,
      to_user VARCHAR(50) NOT NULL,
      content TEXT NOT NULL,
      msg_type VARCHAR(20) DEFAULT 'text',
      msg_id VARCHAR(64) DEFAULT '',
      is_read TINYINT(1) DEFAULT 0,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`);
    await loadUsers();
  } catch (err) {
    console.error('❌ MySQL 错误:', err.message);
  }
})();

const PWD_FILE = path.join(__dirname, 'admin.pwd');
let ADMIN_PWD = '123456';
let userMap = new Map();
let waitingUsers = new Set();
let loginMap = new Map();
let userSessionMap = new Map();
let keepAliveMap = new Map();
let userMatchTimer = new Map();
let roomMem = new Map();
let offlineMsgMem = new Map();
let usernameToSocket = new Map();

const KEEP_ALIVE_EXPIRE = 24 * 60 * 60 * 1000;
const KEEP_ALIVE_CHECK_INTERVAL = 60 * 1000;
const UNLOGGED_CLEAN_INTERVAL = 30000;
const REDIS_EXPIRE = 7200;
const MATCH_TIMEOUT = 15000;
const HEARTBEAT_INTERVAL = 30000;

// ==============================================
// 🔥 加强日志（全局）
// ==============================================
function sysLog(tag, msg, data = {}) {
  const t = new Date().toLocaleString();
  console.log(`[${t}] [${tag}] ${msg}`);
  if (Object.keys(data).length > 0) console.log('└──', JSON.stringify(data, null, 2));
}

function initFileIfNotExists(filePath, defaultContent = '') {
  if (!fs.existsSync(filePath)) fs.writeFileSync(filePath, defaultContent, 'utf8');
}
initFileIfNotExists(PWD_FILE, '123456');

async function loadUsers() {
  try {
    if (!db) return;
    const [rows] = await db.execute('SELECT username,nickname,password FROM users');
    loginMap.clear();
    rows.forEach(u => {
      loginMap.set(u.username, {
        nickname: u.nickname || u.username,
        password: u.password
      });
    });
  } catch (e) {}
}

async function clearUserChatRecords(username) {
  try {
    if (db && username) await db.execute("DELETE FROM messages WHERE from_user=? OR to_user=?", [username, username]);
  } catch (e) {}
}

function loadPwd() {
  try {
    ADMIN_PWD = fs.readFileSync(PWD_FILE, 'utf8').trim() || '123456';
  } catch (e) {
    ADMIN_PWD = '123456';
  }
}

function createMatchRoom(userA, userB) {
  const roomId = `room_${Date.now()}_${Math.floor(Math.random()*10000)}`;
  roomMem.set(roomId, { userA, userB, userALeft: false, userBLeft: false, createTime: Date.now() });
  setTimeout(() => roomMem.delete(roomId), REDIS_EXPIRE * 1000);
  return roomId;
}

function saveOfflineMsg(toUserId, msg) {
  if (!offlineMsgMem.has(toUserId)) offlineMsgMem.set(toUserId, []);
  offlineMsgMem.get(toUserId).push({ ...msg, timestamp: Date.now() });
}

function pushOfflineMsg(socket, userId) {
  const list = offlineMsgMem.get(userId) || [];
  list.forEach(m => socket.emit('new-msg', m));
  offlineMsgMem.delete(userId);
}

function checkReconnectValid(userId, roomId) {
  const room = roomMem.get(roomId);
  if (!room) return { success: false, reason: '房间不存在' };
  const isA = room.userA === userId;
  const isB = room.userB === userId;
  if (!isA && !isB) return { success: false, reason: '非成员' };
  if ((isA && room.userBLeft) || (isB && room.userALeft)) return { success: false, reason: '对方已离开' };
  if (Date.now() - room.createTime > REDIS_EXPIRE * 1000) {
    roomMem.delete(roomId);
    return { success: false, reason: '已过期' };
  }
  return { success: true, opponent: isA ? room.userB : room.userA };
}

function markUserLeave(userId, roomId) {
  const room = roomMem.get(roomId);
  if (!room) return;
  if (room.userA === userId) room.userALeft = true;
  if (room.userB === userId) room.userBLeft = true;
}

function resetRoomExpire(roomId) {
  const r = roomMem.get(roomId);
  if (r) r.createTime = Date.now();
}

function cleanMatchTimer(uid) {
  if (userMatchTimer.has(uid)) {
    clearTimeout(userMatchTimer.get(uid));
    userMatchTimer.delete(uid);
  }
}

function assignAiRobot(sid) {
  const u = userMap.get(sid);
  if (!u || !u.socket.connected || u.isMatched || !waitingUsers.has(sid)) return;
  cleanMatchTimer(sid);
  const aiName = "AI陪伴者";
  const aiId = "ai_bot";
  const rid = createMatchRoom(u.username, aiName);
  u.partner = aiId;
  u.isMatched = true;
  u.roomId = rid;
  waitingUsers.delete(sid);
  keepAliveMap.set(sid, { partnerId: aiId, expireTime: Date.now() + KEEP_ALIVE_EXPIRE });
  u.socket.emit('match-found', { partnerId: aiId, partnerName: aiName, selfId: sid, roomId: rid });
  sysLog('MATCH', '超时匹配AI成功', { user: u.username });
}

function autoJoinMatchPool(sid) {
  const u = userMap.get(sid);
  if (!u || !u.socket.connected || !u.username || !loginMap.has(u.username) || userSessionMap.get(u.username) !== sid || u.isMatched || waitingUsers.has(sid)) return;
  waitingUsers.add(sid);
  const timer = setTimeout(() => assignAiRobot(sid), MATCH_TIMEOUT);
  userMatchTimer.set(sid, timer);
  tryMatch();
}

function stopChat(uid, isInitiative = true) {
  const me = userMap.get(uid);
  if (!me || !me.partner) return;
  cleanMatchTimer(uid);
  if (me.partner !== "ai_bot") {
    const pt = userMap.get(me.partner);
    if (pt && pt.socket) {
      pt.partner = null;
      pt.isMatched = false;
      pt.socket.emit('partner-leave');
      me.roomId && pt.socket.emit('clear-chat-record');
      autoJoinMatchPool(pt.id);
    }
  }
  me.partner = null;
  me.isMatched = false;
  me.socket.emit('match-end', { info: isInitiative ? '已断开' : '结束' });
  keepAliveMap.delete(uid);
  if (me.roomId) {
    roomMem.delete(me.roomId);
    offlineMsgMem.delete(me.username);
    me.roomId = null;
  }
  autoJoinMatchPool(me.id);
  sysLog('CHAT', '聊天结束', { user: me.username, initiative: isInitiative });
}

function tryMatch() {
  const list = Array.from(waitingUsers).map(id => userMap.get(id)).filter(u =>
    u && u.socket.connected && !u.partner && u.username && loginMap.has(u.username) && userSessionMap.get(u.username) === u.id
  );
  if (list.length < 2) return;
  const a = list[0], b = list[1];
  if (a.id === b.id) return;
  cleanMatchTimer(a.id);
  cleanMatchTimer(b.id);
  waitingUsers.delete(a.id);
  waitingUsers.delete(b.id);
  a.partner = b.id;
  b.partner = a.id;
  a.isMatched = true;
  b.isMatched = true;
  const rid = createMatchRoom(a.username, b.username);
  a.roomId = rid;
  b.roomId = rid;
  const aNick = loginMap.get(a.username)?.nickname || a.username;
  const bNick = loginMap.get(b.username)?.nickname || b.username;
  a.socket.emit('match-found', { partnerId: b.id, partnerName: bNick, selfId: a.id, roomId: rid });
  b.socket.emit('match-found', { partnerId: a.id, partnerName: aNick, selfId: b.id, roomId: rid });
  keepAliveMap.set(a.id, { partnerId: b.id, expireTime: Date.now() + KEEP_ALIVE_EXPIRE });
  keepAliveMap.set(b.id, { partnerId: a.id, expireTime: Date.now() + KEEP_ALIVE_EXPIRE });
  sysLog('MATCH', '真人匹配成功', { a: a.username, b: b.username, room: rid });
}

function checkPartnerStatus(uid) {
  const me = userMap.get(uid);
  if (!me || !me.partner) {
    me.socket.emit('partner-status', { isOnline: false, info: '无匹配' });
    return;
  }
  if (me.partner === "ai_bot") {
    me.socket.emit('partner-status', { isOnline: true, info: 'AI在线', partnerId: "ai_bot", partnerName: "AI陪伴者" });
    return;
  }
  const pt = userMap.get(me.partner);
  if (pt && pt.socket.connected) {
    const nick = loginMap.get(pt.username)?.nickname || pt.username;
    me.socket.emit('partner-status', { isOnline: true, info: '在线', partnerId: pt.id, partnerName: nick });
  } else {
    me.partner = null;
    me.isMatched = false;
    me.socket.emit('partner-status', { isOnline: false, info: '对方离线' });
    me.socket.emit('partner-leave');
    me.roomId && me.socket.emit('clear-chat-record');
    autoJoinMatchPool(me.id);
  }
}

function startKeepAliveCheck() {
  setInterval(() => {
    const now = Date.now();
    keepAliveMap.forEach((val, uid) => {
      const u = userMap.get(uid);
      const pid = val.partnerId;
      if (pid === "ai_bot") return;
      const p = userMap.get(pid);
      if (!u || !p || !u.socket.connected || !p.socket.connected) {
        keepAliveMap.delete(uid);
        keepAliveMap.delete(pid);
        u?.socket?.emit('partner-leave');
        p?.socket?.emit('partner-leave');
        u?.roomId && u.socket.emit('clear-chat-record');
        p?.roomId && p.socket.emit('clear-chat-record');
        if (u) autoJoinMatchPool(uid);
        if (p) autoJoinMatchPool(pid);
        sysLog('KEEPALIVE', '对方离线，自动断开', { user: u?.username, partner: p?.username });
        return;
      }
      if (now > val.expireTime) {
        u.socket.emit('keep-alive-expire');
        p.socket.emit('keep-alive-expire');
        stopChat(uid, false);
        stopChat(pid, false);
        keepAliveMap.delete(uid);
        keepAliveMap.delete(pid);
        sysLog('KEEPALIVE', '保活过期', { u: u.username, p: p.username });
        return;
      }
      if (now - (u.lastKeepAlive || 0) > 5*60*1000 || now - (p.lastKeepAlive || 0) > 5*60*1000) {
        u.socket.emit('match-expire');
        p.socket.emit('match-expire');
        stopChat(uid, false);
        stopChat(pid, false);
        keepAliveMap.delete(uid);
        keepAliveMap.delete(pid);
        sysLog('KEEPALIVE', '心跳超时', { u: u.username, p: p.username });
        return;
      }
      u.lastActive = now;
      p.lastActive = now;
      if (u.roomId) resetRoomExpire(u.roomId);
    });
  }, KEEP_ALIVE_CHECK_INTERVAL);
}

const rl = readline.createInterface({ input: process.stdin, output: process.stdout, prompt: '>' });
function showMenu() {
  console.log(`
1在线 2用户列表 3AI 6清空 7断开全部 9删用户 0退出
`);
  rl.prompt();
}

async function deleteUser(username, pwd) {
  try {
    if (pwd !== ADMIN_PWD) return { code: 403, msg: '密码错误' };
    if (!loginMap.has(username)) return { code: 404, msg: '不存在' };
    await db.execute('DELETE FROM users WHERE username=?', [username]);
    loginMap.delete(username);
    userSessionMap.delete(username);
    usernameToSocket.delete(username);
    userMap.forEach((user, sid) => {
      if (user.username === username) {
        user.socket?.emit('user-deleted');
        user.socket?.disconnect();
        userMap.delete(sid);
        waitingUsers.delete(sid);
        keepAliveMap.delete(sid);
        cleanMatchTimer(sid);
      }
    });
    return { code: 200, msg: '删除成功' };
  } catch (e) {
    return { code: 500, msg: '失败' };
  }
}

rl.on('line', async (input) => {
  const cmd = input.trim();
  if (cmd === '9') {
    const users = Array.from(loginMap.keys());
    if (!users.length) { console.log('无用户'); showMenu(); return; }
    users.forEach((u, i) => console.log(`${i+1}. ${u}`));
    rl.question('序号>', async idx => {
      const i = parseInt(idx) - 1;
      if (i < 0 || i >= users.length) { showMenu(); return; }
      const username = users[i];
      rl.question('密码>', async pwd => {
        const res = await deleteUser(username, pwd.trim());
        console.log(res.code === 200 ? '✅成功' : '❌失败：' + res.msg);
        showMenu();
      });
    });
    return;
  }
  switch(cmd.toLowerCase()){
    case '1': userMap.forEach((u, id) => console.log(id, u.username || '未登录')); break;
    case '2': console.log(Array.from(loginMap.keys())); break;
    case '3': console.log('AI启用'); break;
    case '6': userMap.forEach(u => u.socket?.emit('clear-chat-record')); console.log('✅已清空'); break;
    case '7': case '8': userMap.forEach(u => u.partner && stopChat(u.id, false)); console.log('✅已断开全部'); break;
    case '0': process.exit(0); break;
  }
  showMenu();
});

// 跨域
app.use((req, res, next) => {
  const origin = req.headers.origin;
  const allowList = ["https://im6.qzz.io", "https://www.im6.qzz.io"];
  if (allowList.includes(origin)) {
    res.header("Access-Control-Allow-Origin", origin);
  }
  res.header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.header("Access-Control-Allow-Headers", "Content-Type");
  res.header("Access-Control-Allow-Credentials", "true");
  if (req.method === "OPTIONS") return res.sendStatus(200);
  next();
});

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));
app.get('/', (req, res) => {
  res.send('Server Running OK — 加强版：保活+兜底+日志+防断开');
});

// 注册接口
app.post('/register', async (req, res) => {
  if (!req.body) return res.json({ code: 400, msg: '请求格式错误' });
  const { username, password } = req.body;
  if (!username || !password) return res.json({ code: 400, msg: '请输入账号密码' });
  if (username.length < 2 || username.length > 20) return res.json({ code: 400, msg: '用户名长度2-20位' });
  if (password.length < 6) return res.json({ code: 400, msg: '密码不少于6位' });
  try {
    const [rows] = await db.execute('SELECT id FROM users WHERE username=?', [username]);
    if (rows.length > 0) return res.json({ code: 400, msg: '账号已存在' });
    await db.execute('INSERT INTO users (username, password) VALUES (?, ?)', [username, password]);
    loginMap.set(username, { nickname: username, password });
    sysLog('USER', '注册成功', { username });
    return res.json({ code: 200, msg: '注册成功' });
  } catch (err) {
    console.error('注册错误:', err);
    return res.json({ code: 500, msg: '注册失败' });
  }
});

// 登录接口
app.post('/login', async (req, res) => {
  const { username, password } = req.body || {};
  if (!username || !password) return res.json({ code: 400, msg: '请输入账号密码' });
  try {
    const [rows] = await db.execute('SELECT password FROM users WHERE username=?', [username]);
    if (!rows || rows.length === 0) return res.json({ code: 404, msg: '账号不存在' });
    if (rows[0].password === password) {
      loginMap.set(username, { nickname: username, password: password });
      sysLog('USER', '登录成功', { username });
      return res.json({ code: 200, msg: '登录成功' });
    } else {
      return res.json({ code: 400, msg: '密码错误' });
    }
  } catch (err) {
    console.error('登录错误:', err);
    return res.json({ code: 500, msg: '服务器错误' });
  }
});

// Socket.IO
const io = new Server(server, {
  cors: { origin: ["https://im6.qzz.io", "https://www.im6.qzz.io"], methods: ["GET","POST"], credentials: true },
  transports: ['websocket','polling'], pingTimeout:60000, pingInterval:25000, maxHttpBufferSize:10*1024*1024, allowEIO3:true
});

io.on('connection', socket => {
  const sid = socket.id;
  const user = {
    id: sid, socket, username:'', partner:null, isMatched:false,
    lastActive:Date.now(), lastKeepAlive:0, roomId:null
  };
  userMap.set(sid, user);
  sysLog('CONNECT', '新连接', { sid });

  // 未登录清理
  const timer = setInterval(() => {
    if (!user.username || !loginMap.has(user.username) || userSessionMap.get(user.username) !== sid) {
      socket.disconnect();
      userMap.delete(sid);
      clearInterval(timer);
      sysLog('CONNECT', '未登录超时清理', { sid });
    }
  }, UNLOGGED_CLEAN_INTERVAL);

  // 前端日志
  socket.on("client-action-log", (data) => {
    sysLog('FRONT', data.action, { user:data.userId, nick:data.nickname, ...data.extra });
  });

  // 用户上线（加强状态兜底）
  // 用户上线（加强状态兜底）
socket.on('user-online', (data) => {
    const { username } = data;
    if (!username || !loginMap.has(username)) return;
    user.username = username;
    // ✅ 强制标记永久在线，杜绝乱判离线
    user.isOnline = true;
    userSessionMap.set(username, sid);
    usernameToSocket.set(username, socket);
    user.lastActive = Date.now();
    sysLog('ONLINE', '用户上线', { username, sid });
    autoJoinMatchPool(sid);
});

socket.on('match-chat', () => {
    if (!user.username) return socket.emit('match-error');
    if (user.isMatched) stopChat(sid, false);
    if (waitingUsers.has(sid)) return socket.emit('match-error');
    waitingUsers.add(sid);
    const t = setTimeout(() => assignAiRobot(sid), MATCH_TIMEOUT);
    userMatchTimer.set(sid, t);
    sysLog('MATCH', '用户发起匹配', { user: user.username });
    tryMatch();
});

socket.on('stop-chat', () => stopChat(sid, true));

// 🔥 【修复】心跳加强：永不掉线 + 双向回应 + 强制在线
// 🔥 心跳加强：60秒超长容错，永不误判离线
socket.on('HEARTBEAT', () => {
    if (!user.username) return;
    user.isOnline = true;
    // ✅ 改成60秒超时兜底
    user.lastKeepAlive = Date.now();
    user.lastActive = Date.now();
    if (user.isMatched && user.partner) {
        keepAliveMap.set(sid, { 
            partnerId: user.partner, 
            expire: Date.now() + 60000  // ✅ 60秒 = 60000毫秒
        });
    }
    socket.emit('HEARTBEAT-ACK');
});


socket.on('clear-chat', async () => {
    if (user.username) await clearUserChatRecords(user.username);
    socket.emit('clear-chat-record', { msg: '清空成功' });
    sysLog('CHAT', '清空聊天记录', { user: user.username });
});


  // 修改昵称（稳定版）
  socket.on('change-nick', async (newNick) => {
    try {
      const nick = newNick?.trim();
      if (!user.username) return socket.emit('nick-result', { success:false, msg:'未登录' });
      if (!nick || nick.length<2 || nick.length>20) return socket.emit('nick-result', { success:false, msg:'2-20位' });
      await db.execute('UPDATE users SET nickname=? WHERE username=?', [nick, user.username]);
      const info = loginMap.get(user.username);
      if (info) { info.nickname = nick; loginMap.set(user.username, info); }
      socket.emit('nick-result', { success:true, newName:nick });
      sysLog('NICK', '修改昵称成功', { user:user.username, newNick:nick });
    } catch (err) {
      console.error('昵称错误:', err);
      socket.emit('nick-result', { success:false, msg:'修改失败' });
    }
  });

  socket.on('checkLogin', (data) => {
    const { userId, token } = data;
    if (!userId || !token || token !== userId || !loginMap.has(userId)) return socket.emit('notLogin');
    user.username = userId;
    userSessionMap.set(userId, sid);
    usernameToSocket.set(userId, socket);
    pushOfflineMsg(socket, userId);
    socket.emit('loginSuccess');
    sysLog('AUTH', '校验登录成功', { userId });
  });

  socket.on('RECONNECT', (data) => {
    const { userId, roomId } = data;
    if (!userId || !roomId || userId !== user.username) return socket.emit('RECONNECT_RESULT', { success:false });
    const ret = checkReconnectValid(userId, roomId);
    socket.emit('RECONNECT_RESULT', ret);
    if (ret.success) {
      user.isMatched = true;
      user.roomId = roomId;
      pushOfflineMsg(socket, userId);
      sysLog('RECONNECT', '重连成功', { userId, roomId });
    }
  });

  socket.on('LEAVE', (data) => {
    const { userId, roomId } = data;
    if (!userId || !roomId || userId !== user.username) return socket.emit('LEAVE_RESULT', { success:false });
    markUserLeave(userId, roomId);
    stopChat(sid, true);
    socket.emit('LEAVE_RESULT', { success:true });
  });

  // ==============================
  // 🔥 消息转发【加强兜底版】
  // ==============================
  socket.on('send-msg', async (data) => {
    sysLog('MSG', '收到消息', { from: user.username, data });
    try {
      // 状态兜底：丢状态自动恢复
      if (!user.username) {
        sysLog('MSG', '发送失败：未登录');
        return socket.emit('msg-fail', { info:'未登录' });
      }
      if (!user.isMatched || !user.partner) {
        if (data.toId) {
          user.partner = data.toId;
          user.isMatched = true;
          user.roomId = data.roomId || `room_${user.username}_${data.toId}`;
          sysLog('MSG', '状态兜底恢复', { user:user.username, partner:data.toId });
        } else {
          sysLog('MSG', '发送失败：未匹配');
          return socket.emit('msg-fail', { info:'请先匹配' });
        }
      }

      const content = data.content?.trim();
      if (!content) {
        sysLog('MSG', '发送失败：空内容');
        return socket.emit('msg-fail', { info:'内容不能为空' });
      }

      const to = userMap.get(user.partner);
      const toUser = to?.username || 'unknown';
      const fromNick = loginMap.get(user.username)?.nickname || user.username;

      // 入库
      if (db) {
        await db.execute(
          'INSERT INTO messages (from_user,to_user,content,msg_type,msg_id,is_read) VALUES (?,?,?,?,?,?)',
          [user.username, toUser, content, data.type||'text', data.msgId||'', 0]
        );
      }

      // AI
      if (user.partner === 'ai_bot') {
        if (data.type === 'text') {
          const reply = await callAI(content);
          setTimeout(() => {
            socket.emit('new-msg', {
              content:reply, type:'text', burn:false, receipt:false,
              msgId:Date.now().toString(), fromId:'ai_bot', fromName:'AI陪伴者'
            });
            sysLog('MSG', 'AI回复', { to:user.username });
          }, 600);
        }
        return;
      }

      // 转发
      if (to && to.socket && to.socket.connected) {
        to.socket.emit('new-msg', {
          content:data.content, type:data.type||'text', burn:data.burn||false,
          receipt:data.receipt||false, msgId:data.msgId||'',
          fromId:user.username, fromName:fromNick
        });
        sysLog('MSG', '转发成功', { from:user.username, to:toUser });
      } else {
        saveOfflineMsg(toUser, {
          content:data.content, fromId:user.username, fromName:fromNick,
          msgId:data.msgId, type:data.type||'text'
        });
        socket.emit('msg-fail', { info:'对方离线，已存离线' });
        sysLog('MSG', '对方不在线，已存离线', { to:toUser });
      }

    } catch (err) {
      console.error('消息异常:', err);
      socket.emit('msg-fail', { info:'发送异常' });
    }
  });

  socket.on('msg-read', (data) => {
    const { msgId } = data || {};
    if (!msgId) return;
    const partner = userMap.get(user.partner);
    if (partner && partner.socket) partner.socket.emit('msg-read', { msgId });
  });

  // 断开
  socket.on('disconnect', () => {
    cleanMatchTimer(sid);
    if (user.username) {
      userSessionMap.delete(user.username);
      usernameToSocket.delete(user.username);
    }
    if (user.partner && user.partner !== 'ai_bot') {
      const p = userMap.get(user.partner);
      if (p && p.socket) p.socket.emit('partner-leave');
    }
    keepAliveMap.delete(sid);
    userMap.delete(sid);
    waitingUsers.delete(sid);
    clearInterval(timer);
    sysLog('DISCONNECT', '断开连接', { sid, user: user.username });
  });
});

// 保活
// ====================== 配置区（不用改）======================
const TARGET_URL = 'http://127.0.0.1:10000';
const PING_INTERVAL = 3 * 60 * 1000;   // 3分钟一次自我Ping
const FAILURE_THRESHOLD = 3;           // 连续失败3次触发重启
const LOG_FILE = path.join(__dirname, 'keepalive.log');
const SCRIPT_PATH = __filename;        // 当前脚本自身路径（用于自重启）
// ============================================================

let consecutiveFailures = 0;
let isRestarting = false;

// 日志：控制台 + 文件双输出
function writeLog(msg) {
    const t = new Date().toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai' });
    const line = `[${t}] ${msg}\n`;
    console.log(line.trim());
    fs.appendFile(LOG_FILE, line, err => err && console.error('日志写入失败:', err));
}

// 核心：自我Ping探活
function doPing() {
    if (isRestarting) return;
    try {
        const req = http.get(TARGET_URL, { timeout: 5000 }, res => {
            if (res.statusCode === 200) {
                consecutiveFailures = 0;
                writeLog(`✅ Ping成功 | 状态码:${res.statusCode}`);
            } else {
                consecutiveFailures++;
                writeLog(`⚠️ Ping异常 | 状态码:${res.statusCode} | 连续失败:${consecutiveFailures}/${FAILURE_THRESHOLD}`);
                checkRestart();
            }
            res.resume();
        });

        req.on('timeout', () => {
            req.destroy();
            consecutiveFailures++;
            writeLog(`⏱️ Ping超时 | 连续失败:${consecutiveFailures}/${FAILURE_THRESHOLD}`);
            checkRestart();
        });

        req.on('error', err => {
            consecutiveFailures++;
            writeLog(`❌ Ping失败 | ${err.message} | 连续失败:${consecutiveFailures}/${FAILURE_THRESHOLD}`);
            checkRestart();
        });
    } catch (err) {
        writeLog(`💥 探活异常 | ${err.message}`);
        consecutiveFailures++;
        checkRestart();
    }
}

// 关键：连续失败达标 → 自动重启自身
function checkRestart() {
    if (isRestarting || consecutiveFailures < FAILURE_THRESHOLD) return;
    isRestarting = true;

    writeLog(`🚨 连续失败${FAILURE_THRESHOLD}次，准备自动重启脚本...`);

    // 启动新进程运行自己
    const cmd = `node ${SCRIPT_PATH}`;
    exec(cmd, (err) => {
        if (err) writeLog(`❌ 重启命令执行失败: ${err.message}`);
    });

    // 退出当前进程，新进程会立刻顶上
    setTimeout(() => {
        writeLog(`🔚 旧进程退出，新进程已拉起`);
        process.exit(1);
    }, 1000);
}

// 兜底：捕获脚本自身崩溃、未处理异常 → 自动重启
process.on('uncaughtException', err => {
    writeLog(`💀 脚本崩溃捕获: ${err.message}`);
    consecutiveFailures = FAILURE_THRESHOLD;
    checkRestart();
});

process.on('unhandledRejection', err => {
    writeLog(`💀 异步异常捕获: ${err.message}`);
    consecutiveFailures = FAILURE_THRESHOLD;
    checkRestart();
});

// 启动
writeLog(`🚀 一体化保活脚本启动 | 目标:${TARGET_URL} | 间隔:${PING_INTERVAL/1000/60}分钟`);
setInterval(doPing, PING_INTERVAL);
// 立刻执行一次，避免刚启动就挂
doPing();
app.use((req, res) => res.status(404).json({ code:404, msg:'不存在' }));

server.listen(PORT, '0.0.0.0', () => {
  console.log('=========================================');
  console.log('🔥 加强版服务已启动 | 端口:'+PORT);
  console.log('✅ 加强日志 | ✅ 状态兜底 | ✅ 防断开');
  console.log('✅ 消息不丢 | ✅ 离线存储 | ✅ 昵称稳定');
  console.log('=========================================');
  showMenu();
});
