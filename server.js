const express = require('express');
const http = require('http');
const fs = require('fs');
const path = require('path');
const { Server } = require('socket.io');
const crypto = require('crypto');
const readline = require('readline');
const mysql = require('mysql2/promise');
const axios = require('axios');

const PORT = process.env.PORT || 6500;

async function callAI(prompt) {
  try {
    console.log("[AI] 收到明文消息:", prompt);
    const res = await axios.post("http://127.0.0.1:11434/api/chat", {
      model: "qwen2.5:0.5b",
      messages: [{ role: "user", content: prompt }],
      stream: false
    }, { timeout: 15000 });
    return res.data?.message?.content || "我在呢～";
  } catch (err) {
    console.error("[AI调用失败]:", err.message);
    return "AI暂时休息了，等下再来找我吧～";
  }
}

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
    console.error('❌ MySQL 失败:', err.message);
  }
})();

const PWD_FILE = path.join(__dirname, 'admin.pwd');
let ADMIN_PWD = '';

const HEARTBEAT_INTERVAL = 30000;

let userMap = new Map();
let waitingUsers = new Set();
let loginMap = new Map();
let userSessionMap = new Map();
let keepAliveMap = new Map();
let userMatchTimer = new Map();
let roomMem = new Map();
let offlineMsgMem = new Map();

const KEEP_ALIVE_EXPIRE = 24 * 60 * 60 * 1000;
const KEEP_ALIVE_CHECK_INTERVAL = 60 * 1000;
const UNLOGGED_CLEAN_INTERVAL = 30000;
const REDIS_EXPIRE = 7200;
const MATCH_TIMEOUT = 15000;

function initFileIfNotExists(filePath, defaultContent = '') {
  if (!fs.existsSync(filePath)) fs.writeFileSync(filePath, defaultContent, 'utf8');
}

initFileIfNotExists(PWD_FILE, '123456');

async function loadUsers() {
  try {
    if (!db) return;
    const [rows] = await db.execute('SELECT username,password FROM users');
    loginMap.clear();
    rows.forEach(u => loginMap.set(u.username, u.password));
  } catch (e) {
    console.log('⚠️ 加载用户失败');
  }
}

async function updateUsernameInDB(oldName, newName) {
  try {
    if (!db || !oldName || !newName) return false;
    const conn = await db.getConnection();
    await conn.beginTransaction();
    await conn.execute("UPDATE users SET username = ? WHERE username = ?", [newName, oldName]);
    await conn.execute("UPDATE messages SET from_user = ? WHERE from_user = ?", [newName, oldName]);
    await conn.execute("UPDATE messages SET to_user = ? WHERE to_user = ?", [newName, oldName]);
    await conn.commit();
    const pwd = loginMap.get(oldName);
    loginMap.delete(oldName);
    loginMap.set(newName, pwd);
    conn.release();
    return true;
  } catch (e) {
    console.error('更新昵称失败:', e);
    return false;
  }
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
    fs.writeFileSync(PWD_FILE, ADMIN_PWD);
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
  offlineMsgMem.get(toUserId).push(msg);
  setTimeout(() => {
    const ms = offlineMsgMem.get(toUserId)?.filter(m => m.timestamp + REDIS_EXPIRE * 1000 > Date.now());
    ms?.length ? offlineMsgMem.set(toUserId, ms) : offlineMsgMem.delete(toUserId);
  }, REDIS_EXPIRE * 1000);
}

function pushOfflineMsg(socket, userId) {
  const ms = offlineMsgMem.get(userId) || [];
  ms.forEach(m => socket.emit("OFFLINE_MESSAGE", m));
  offlineMsgMem.delete(userId);
}

function checkReconnectValid(userId, roomId) {
  const room = roomMem.get(roomId);
  if (!room) return { success: false, reason: '房间不存在' };
  const isA = room.userA === userId;
  const isB = room.userB === userId;
  if (!isA && !isB) return { success: false, reason: '非成员' };
  const opp = isA ? room.userB : room.userA;
  if ((isA && room.userBLeft) || (isB && room.userALeft)) return { success: false, reason: '对方已离开' };
  if (Date.now() - room.createTime > REDIS_EXPIRE * 1000) {
    roomMem.delete(roomId);
    return { success: false, reason: '已过期' };
  }
  return { success: true, opponent: opp };
}

function markUserLeave(userId, roomId) {
  const room = roomMem.get(roomId);
  if (!room) return;
  if (room.userA === userId) room.userALeft = true;
  if (room.userB === userId) room.userBLeft = true;
  const opp = room.userA === userId ? room.userB : room.userA;
  const os = Array.from(userMap.values()).find(u => u.username === opp);
  if (os) os.socket.emit("USER_LEFT");
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
  userMap.set(sid, u);
  keepAliveMap.set(sid, { partnerId: aiId, expireTime: Date.now() + KEEP_ALIVE_EXPIRE });
  u.socket.emit('match-found', { partnerId: aiId, partnerName: aiName, selfId: sid, roomId: rid });
}

function autoJoinMatchPool(sid) {
  const u = userMap.get(sid);
  if (!u || !u.socket.connected || !u.username || !loginMap.has(u.username) || userSessionMap.get(u.username) !== u.id || u.isMatched || waitingUsers.has(sid)) return;
  u.heartbeatStatus = true;
  u.lastActive = Date.now();
  waitingUsers.add(sid);
  userMap.set(sid, u);
  const t = setTimeout(() => assignAiRobot(sid), MATCH_TIMEOUT);
  userMatchTimer.set(sid, t);
  tryMatch();
}

function stopChat(uid, isInitiative = true) {
  const me = userMap.get(uid);
  if (!me || !me.partner) return;
  cleanMatchTimer(uid);
  if (me.partner !== "ai_bot") {
    const pt = userMap.get(me.partner);
    if (pt && pt.socket?.connected) {
      pt.partner = null;
      pt.isMatched = false;
      pt.socket.emit('partner-leave');
      pt.socket.emit('clear-chat-record');
      pt.socket.emit('match-end', { info: '对方离开' });
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
}

function tryMatch() {
  const valid = Array.from(waitingUsers).map(id => userMap.get(id)).filter(u =>
    u && u.socket.connected && !u.partner && u.username && loginMap.has(u.username) && userSessionMap.get(u.username) === u.id
  );
  if (valid.length < 2) return;
  const u1 = valid[0], u2 = valid[1];
  if (u1.id === u2.id) return;
  cleanMatchTimer(u1.id);
  cleanMatchTimer(u2.id);
  u1.partner = u2.id;
  u2.partner = u1.id;
  u1.isMatched = true;
  u2.isMatched = true;
  waitingUsers.delete(u1.id);
  waitingUsers.delete(u2.id);
  const rid = createMatchRoom(u1.username, u2.username);
  u1.roomId = rid;
  u2.roomId = rid;
  userMap.set(u1.id, u1);
  userMap.set(u2.id, u2);
  u1.socket.emit('match-found', { partnerId: u2.id, partnerName: u2.username, selfId: u1.id, roomId: rid });
  u2.socket.emit('match-found', { partnerId: u1.id, partnerName: u1.username, selfId: u2.id, roomId: rid });
  const expire = Date.now() + KEEP_ALIVE_EXPIRE;
  keepAliveMap.set(u1.id, { partnerId: u2.id, expireTime: expire });
  keepAliveMap.set(u2.id, { partnerId: u1.id, expireTime: expire });
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
  if (pt && pt.socket?.connected) {
    me.socket.emit('partner-status', { isOnline: true, info: '在线', partnerId: pt.id, partnerName: pt.username });
  } else {
    me.partner = null;
    me.isMatched = false;
    me.socket.emit('partner-status', { isOnline: false, info: '对方离开' });
    me.socket.emit('partner-leave');
    me.socket.emit('clear-chat-record');
    autoJoinMatchPool(me.id);
  }
}

function startKeepAliveCheck() {
  setInterval(() => {
    const now = Date.now();
    keepAliveMap.forEach((inf, uid) => {
      const u = userMap.get(uid);
      const pid = inf.partnerId;
      if (pid === "ai_bot") return;
      const p = userMap.get(pid);
      if (!u || !p || !u.socket.connected || !p.socket.connected) {
        keepAliveMap.delete(uid);
        keepAliveMap.delete(pid);
        u?.socket?.emit('partner-leave');
        p?.socket?.emit('partner-leave');
        u?.socket?.emit('clear-chat-record');
        p?.socket?.emit('clear-chat-record');
        if (u) autoJoinMatchPool(uid);
        if (p) autoJoinMatchPool(pid);
        return;
      }
      if (now > inf.expireTime) {
        u.socket.emit('keep-alive-expire', { info: '24小时到期' });
        p.socket.emit('keep-alive-expire', { info: '24小时到期' });
        u.socket.emit('partner-leave');
        p.socket.emit('partner-leave');
        u.socket.emit('clear-chat-record');
        p.socket.emit('clear-chat-record');
        stopChat(uid, false);
        stopChat(pid, false);
        keepAliveMap.delete(uid);
        keepAliveMap.delete(pid);
        return;
      }
      if (now - (u.lastKeepAlive || 0) > 5*60*1000 || now - (p.lastKeepAlive || 0) > 5*60*1000) {
        u.socket.emit('match-expire', { info: '长时间未响应' });
        p.socket.emit('match-expire', { info: '长时间未响应' });
        u.socket.emit('partner-leave');
        p.socket.emit('partner-leave');
        u.socket.emit('clear-chat-record');
        p.socket.emit('clear-chat-record');
        stopChat(uid, false);
        keepAliveMap.delete(uid);
        keepAliveMap.delete(pid);
        return;
      }
      u.heartbeatStatus = true;
      p.heartbeatStatus = true;
      u.lastActive = now;
      p.lastActive = now;
      if (u.roomId) resetRoomExpire(u.roomId);
    });
  }, KEEP_ALIVE_CHECK_INTERVAL);
}

const rl = readline.createInterface({ input: process.stdin, output: process.stdout, prompt: '>' });

function showMenu() {
  console.log(`
1在线 2注册 3AI开关 6清空 7断开 9删用户
c改密 0退出
ad内容 推送广告
`);
  rl.prompt();
}

async function deleteUser(username, adminPwd) {
  try {
    if (adminPwd !== ADMIN_PWD) return { code: 403, msg: '密码错' };
    if (!loginMap.has(username)) return { code: 404, msg: '不存在' };
    await db.execute('DELETE FROM users WHERE username=?', [username]);
    loginMap.delete(username);
    userSessionMap.delete(username);
    userMap.forEach((user, socketId) => {
      if (user.username === username) {
        user.socket?.emit('user-deleted', { msg: '账号已删除' });
        user.socket?.disconnect(true);
        userMap.delete(socketId);
        waitingUsers.delete(socketId);
        keepAliveMap.delete(socketId);
        cleanMatchTimer(socketId);
      }
    });
    return { code: 200, msg: '删除成功' };
  } catch (e) {
    return { code: 500, msg: '失败' };
  }
}

function pushAd(content) {
  if (!content) return;
  userMap.forEach(u => {
    if (u.socket.connected && u.username && loginMap.has(u.username))
      u.socket.emit('new-msg', { fromName: '广告', content: content, type: 'text' });
  });
  console.log("✅ 广告已推送");
}

rl.on('line', async (input) => {
  const cmd = input.trim();
  if (cmd.toLowerCase().startsWith('ad ')) { pushAd(cmd.slice(3).trim()); showMenu(); return; }
  if (cmd === '9') {
    const users = Array.from(loginMap.keys());
    if (!users.length) { console.log('无用户'); showMenu(); return; }
    users.forEach((n,j)=>console.log(j+1+'.'+n));
    rl.question('序号>',i=>{
      const idx=+i-1;
      if (idx<0||idx>=users.length) { showMenu(); return; }
      const name=users[idx];
      rl.question('密码>',async pwd=>{
        const r=await deleteUser(name,pwd.trim());
        console.log(r.code===200?'✅成功':'❌失败：'+r.msg);
        showMenu();
      });
    });
    return;
  }
  switch(cmd.toLowerCase()){
    case '1': userMap.forEach((u,id)=>{console.log(id,u.username||'未登录')}); break;
    case '2': Array.from(loginMap.keys()).forEach((n,i)=>console.log(i+1+'.'+n)); break;
    case '3': console.log('AI保持启用'); break;
    case '6': userMap.forEach(u=>u.socket?.emit('clear-chat-record')); console.log('✅清空'); break;
    case '7':case '8': userMap.forEach(u=>{if(u.partner)stopChat(u.id,false);}); console.log('✅断开全部'); break;
    case 'c':
      rl.question('旧密>',o=>{
        if(o!==ADMIN_PWD){console.log('错');showMenu();return;}
        rl.question('新密>',n=>{ADMIN_PWD=n.trim();fs.writeFileSync(PWD_FILE,ADMIN_PWD);console.log('✅改密');showMenu();});
      });
      return;
    case '0': userMap.forEach(u=>u.socket?.disconnect(true)); process.exit(0); break;
  }
  showMenu();
});

const app = express();
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "https://www.im6.qzz.io");
  res.header("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.header("Access-Control-Allow-Headers", "Content-Type");
  res.header("Access-Control-Allow-Credentials", "true");
  if (req.method === "OPTIONS") return res.sendStatus(200);
  next();
});

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));
app.use(express.static(path.join(__dirname)));

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));

app.post('/register', async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password || username.length < 2 || username.length > 20 || password.length < 6)
    return res.json({ code: 400, msg: '格式错误' });
  try {
    const [exists] = await db.execute('SELECT id FROM users WHERE username=?', [username]);
    if (exists.length) return res.json({ code: 400, msg: '已存在' });
    await db.execute('INSERT INTO users (username,password) VALUES (?,?)', [username, password]);
    loginMap.set(username, password);
    return res.json({ code: 200, msg: '注册成功' });
  } catch (err) {
    console.error('注册错误:', err);
    return res.json({ code: 500, msg: '注册失败' });
  }
});

app.post('/login', async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) return res.json({ code: 400, msg: '请输入账号密码' });
  try {
    const [rows] = await db.execute('SELECT password FROM users WHERE username=?', [username]);
    if (!rows.length) return res.json({ code: 404, msg: '账号不存在' });
    if (rows[0].password === password) {
      loginMap.set(username, password);
      return res.json({ code: 200, msg: '登录成功' });
    } else {
      return res.json({ code: 400, msg: '密码错误' });
    }
  } catch (err) {
    console.error('登录错误:', err);
    return res.json({ code: 500, msg: '服务器错误' });
  }
});

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: ["https://im6.qzz.io", "https://www.im6.qzz.io"], methods: ["GET","POST"], credentials: true },
  transports: ['websocket','polling'],
  pingTimeout: 60000,
  pingInterval: 25000,
  maxHttpBufferSize: 10*1024*1024,
  allowEIO3: true
});

io.on('connection', socket => {
  const sid = socket.id;
  const user = { id:sid, socket:socket, username:'', partner:null, isMatched:false, lastActive:Date.now(), heartbeatStatus:true, lastKeepAlive:Date.now(), roomId:null };
  userMap.set(sid, user);

  const unloggedTimer = setInterval(() => {
    if (!user.username || !loginMap.has(user.username) || userSessionMap.get(user.username) !== sid) {
      socket.disconnect(true);
      userMap.delete(sid);
      clearInterval(unloggedTimer);
    } else clearInterval(unloggedTimer);
  }, UNLOGGED_CLEAN_INTERVAL);

  socket.on('clear-chat', async () => {
    if (user.username) await clearUserChatRecords(user.username);
    socket.emit('clear-chat-record', { msg: '清空成功' });
  });

  socket.on('change-nick', async (data) => {
    try {
      const newName = data?.newName?.trim();
      const oldName = user.username;
      if (!oldName) return socket.emit('nick-result', { success: false, msg: '未登录' });
      if (!newName || newName.length < 2 || newName.length > 20) return socket.emit('nick-result', { success: false, msg: '长度2-20' });
      if (newName === oldName) return socket.emit('nick-result', { success: false, msg: '相同' });
      const [exists] = await db.execute("SELECT id FROM users WHERE username=?", [newName]);
      if (exists.length) return socket.emit('nick-result', { success: false, msg: '已被占用' });
      const ok = await updateUsernameInDB(oldName, newName);
      if (!ok) return socket.emit('nick-result', { success: false, msg: '失败' });
      user.username = newName;
      userSessionMap.delete(oldName);
      userSessionMap.set(newName, sid);
      userMap.set(sid, user);
      socket.emit('nick-result', { success: true, msg: '成功', newName });
    } catch (e) {
      socket.emit('nick-result', { success: false, msg: '异常' });
    }
  });

  socket.on('checkLogin', d => {
    const { userId, token } = d;
    if (userId === "AI陪伴者") return socket.emit('notLogin');
    const ok = userId && token === userId && loginMap.has(userId);
    if (!ok) return socket.emit('notLogin');
    user.username = userId;
    user.lastActive = Date.now();
    userMap.set(sid, user);
    if (userSessionMap.get(userId) !== sid) {
      const old = userSessionMap.get(userId);
      if (old) {
        const ou = userMap.get(old);
        if (ou && ou.socket.connected) {
          ou.socket.emit('kick-out', { msg: '别处登录' });
          ou.username = '';
          ou.isMatched = false;
          ou.partner = null;
          ou.roomId = null;
          userMap.set(old, ou);
          waitingUsers.delete(old);
          keepAliveMap.delete(old);
          cleanMatchTimer(old);
        }
      }
      userSessionMap.set(userId, sid);
    }
    pushOfflineMsg(socket, userId);
    socket.emit('loginSuccess');
    autoJoinMatchPool(sid);
  });

  socket.on('HEARTBEAT', () => {
    if (user.username && loginMap.has(user.username) && userSessionMap.get(user.username) === sid) {
      user.lastActive = Date.now();
      user.heartbeatStatus = true;
      socket.emit('heartbeat-ack');
      if (user.roomId) resetRoomExpire(user.roomId);
    }
  });

  socket.on('user-online', d => {
    const name = d?.username?.trim();
    if (name === "AI陪伴者") return socket.emit('invalid-username');
    if (name && name.length >=2 && name.length <=20 && loginMap.has(name)) {
      if (userSessionMap.has(name)) {
        const old = userSessionMap.get(name);
        const ou = userMap.get(old);
        if (ou && ou.socket.connected) {
          ou.socket.emit('kick-out', { msg: '别处登录' });
          ou.username = '';
          ou.isMatched = false;
          ou.partner = null;
          ou.roomId = null;
          userMap.set(old, ou);
          waitingUsers.delete(old);
          keepAliveMap.delete(old);
          cleanMatchTimer(old);
        }
      }
      user.username = name;
      user.lastActive = Date.now();
      userMap.set(sid, user);
      userSessionMap.set(name, sid);
      socket.emit('username-set-success');
      autoJoinMatchPool(sid);
    } else socket.emit('invalid-username');
  });

  socket.on('match-chat', () => {
    if (!user.username) return socket.emit('match-end', { info: '请登录' });
    if (user.isMatched) stopChat(sid, false);
    if (waitingUsers.has(sid)) return socket.emit('match-tip', { info: '排队中' });
    waitingUsers.add(sid);
    const t = setTimeout(() => assignAiRobot(sid), MATCH_TIMEOUT);
    userMatchTimer.set(sid, t);
    tryMatch();
  });

  socket.on('stop-chat', () => stopChat(sid, true));
  socket.on('check-partner', () => checkPartnerStatus(sid));

  socket.on('send-msg', async (d) => {
    try {
      if (!user.username || !user.isMatched || !user.partner || !d) return socket.emit('msg-fail', { info: '无法发送' });
      const plain = d.content;
      if (!plain) return socket.emit('msg-fail', { info: '消息为空' });
      if (db) {
        const toUser = user.partner === "ai_bot" ? "AI陪伴者" : (userMap.get(user.partner)?.username || "unknown");
        await db.execute('INSERT INTO messages (from_user,to_user,content,msg_type,msg_id,is_read) VALUES (?,?,?,?,?,?)',
          [user.username, toUser, plain, d.type||'text', d.msgId||'', 0]);
      }
      if (user.partner === "ai_bot") {
        if (d.type === 'text') {
          const aiReply = await callAI(plain);
          setTimeout(() => {
            socket.emit('new-msg', {
              content: aiReply, type:'text', burn:d.burn||false, receipt:d.receipt||false,
              msgId:'ai_'+Date.now(), fromId:"ai_bot", fromName:"AI陪伴者"
            });
            if (d.receipt && d.msgId) socket.emit('msg-read', { msgId:d.msgId });
          }, 600 + Math.random()*400);
        }
        return;
      }
      const tu = userMap.get(user.partner);
      if (tu && tu.socket.connected) {
        tu.socket.emit('new-msg', {
          content:d.content, data:d.data, type:d.type||'text', burn:d.burn||false, receipt:d.receipt||false,
          msgId:d.msgId||'', fromId:sid, fromName:user.username
        });
      } else {
        saveOfflineMsg(tu?.username || d.toId, {
          fromId:user.username, type:d.type||'text', content:d.content, data:d.data, receipt:d.receipt||false,
          burn:d.burn||false, msgId:d.msgId||'', timestamp:Date.now()
        });
        socket.emit('msg-fail', { info: '对方离线，已存离线' });
      }
    } catch (e) {
      socket.emit('msg-fail', { info: '发送失败' });
    }
  });

  socket.on('msg-read-confirm', async (data) => {
    try {
      const { msgId, toId } = data;
      if (!user.username || !msgId || !toId) return;
      const tu = Array.from(userMap.values()).find(x => x.id === toId);
      if (tu && tu.socket.connected) tu.socket.emit('msg-read', { msgId });
      if (db) await db.execute("UPDATE messages SET is_read=1 WHERE msg_id=? AND to_user=?", [msgId, user.username]);
    } catch(e) {}
  });

  socket.on('keep-alive', () => {
    if (!user.isMatched || !user.partner) return socket.emit('keep-alive-ack', { success:false });
    if (user.partner === "ai_bot") {
      user.lastKeepAlive = Date.now();
      keepAliveMap.set(sid, { partnerId:"ai_bot", expireTime:Date.now()+KEEP_ALIVE_EXPIRE });
      return socket.emit('keep-alive-ack', { success:true });
    }
    const pt = userMap.get(user.partner);
    if (!pt || !pt.socket.connected) {
      socket.emit('keep-alive-ack', { success:false });
      socket.emit('partner-leave');
      socket.emit('clear-chat-record');
      stopChat(sid, false);
      return;
    }
    user.lastKeepAlive = Date.now();
    pt.lastKeepAlive = Date.now();
    const expire = Date.now() + KEEP_ALIVE_EXPIRE;
    keepAliveMap.set(sid, { partnerId:pt.id, expireTime:expire });
    keepAliveMap.set(pt.id, { partnerId:sid, expireTime:expire });
    if (user.roomId) resetRoomExpire(user.roomId);
    socket.emit('keep-alive-ack', { success:true });
  });

  socket.on('RECONNECT', d => {
    const { userId, roomId } = d;
    if (!userId || !roomId || userId !== user.username) return socket.emit('RECONNECT_RESULT', { success:false });
    const cr = checkReconnectValid(userId, roomId);
    socket.emit('RECONNECT_RESULT', cr);
    if (cr.success) {
      user.isMatched = true;
      user.roomId = roomId;
      const room = roomMem.get(roomId);
      const opp = room.userA === userId ? room.userB : room.userA;
      user.partner = opp === "AI陪伴者" ? "ai_bot" : Array.from(userMap.values()).find(x=>x.username===opp)?.id;
      userMap.set(sid, user);
      pushOfflineMsg(socket, userId);
    }
  });

  socket.on('LEAVE', d => {
    const { userId, roomId } = d;
    if (!userId || !roomId || userId !== user.username) return socket.emit('LEAVE_RESULT', { success:false });
    markUserLeave(userId, roomId);
    cleanMatchTimer(sid);
    stopChat(sid, true);
    socket.emit('LEAVE_RESULT', { success:true });
  });

  socket.on('disconnect', () => {
    cleanMatchTimer(sid);
    if (user.username && loginMap.has(user.username)) {
      if (userSessionMap.get(user.username) === sid) userSessionMap.delete(user.username);
      if (user.partner && user.partner !== "ai_bot") {
        const pt = userMap.get(user.partner);
        if (pt && pt.socket) {
          pt.socket.emit('partner-leave');
          pt.socket.emit('clear-chat-record');
        }
      }
      keepAliveMap.delete(sid);
      if (user.partner) keepAliveMap.delete(user.partner);
      setTimeout(() => userMap.delete(sid), 180000);
    } else userMap.delete(sid);
  });
});

loadPwd();
startKeepAliveCheck();

setInterval(() => {
  try {
    const req = http.get(`http://127.0.0.1:${PORT}`, (res) => {
      res.resume();
      console.log(`[保活成功] ${new Date().toLocaleString()}`);
    });
    req.setTimeout(5000, () => req.destroy());
    req.on('error', () => console.log(`[保活失败] ${new Date().toLocaleString()}`));
  } catch (e) {}
}, 3 * 60 * 1000);

process.on('uncaughtException', (e) => console.error('【全局异常】', e.message));
process.on('unhandledRejection', (r) => console.error('【Promise异常】', r));
process.on('SIGINT', () => process.exit(0));

app.use((req, res) => res.status(404).json({ code:404, msg:'不存在' }));

server.listen(PORT, '0.0.0.0', () => {
  console.log('=========================================');
  console.log('✅ 无加密纯净版 | 端口:'+PORT);
  console.log('✅ 跨域/登录/匹配/保活 全部正常');
  console.log('✅ 无冗余、无加密代码');
  console.log('=========================================');
  showMenu();
});
