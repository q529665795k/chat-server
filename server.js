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
        return;
      }
      if (now > val.expireTime) {
        u.socket.emit('keep-alive-expire');
        p.socket.emit('keep-alive-expire');
        stopChat(uid, false);
        stopChat(pid, false);
        keepAliveMap.delete(uid);
        keepAliveMap.delete(pid);
        return;
      }
      if (now - (u.lastKeepAlive || 0) > 5*60*1000 || now - (p.lastKeepAlive || 0) > 5*60*1000) {
        u.socket.emit('match-expire');
        p.socket.emit('match-expire');
        stopChat(uid, false);
        stopChat(pid, false);
        keepAliveMap.delete(uid);
        keepAliveMap.delete(pid);
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

// 跨域：按你要求：带www + 不带www 都允许
// ====================== 还原成你原来能用的跨域 ======================
app.use((req, res, next) => {
  const origin = req.headers.origin;
  const allowList = ["https://im6.qzz.io", "https://www.im6.qzz.io"];

  // 动态设置，不写死！
  if (allowList.includes(origin)) {
    res.header("Access-Control-Allow-Origin", origin);
  }

  res.header("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.header("Access-Control-Allow-Headers", "Content-Type");
  res.header("Access-Control-Allow-Credentials", "true");

  if (req.method === "OPTIONS") return res.sendStatus(200);
  next();
});




// ======================================================================


app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

app.get('/', (req, res) => {
  res.send('Server Running OK — 已修复所有问题');
});

app.post('/register', async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password || username.length < 2 || username.length > 20 || password.length < 6)
    return res.json({ code: 400, msg: '格式错误' });
  try {
    const [exists] = await db.execute('SELECT id FROM users WHERE username=?', [username]);
    if (exists.length) return res.json({ code: 400, msg: '已存在' });
    await db.execute('INSERT INTO users (username,nickname,password) VALUES (?,?,?)', [username, username, password]);
    loginMap.set(username, { nickname: username, password });
    return res.json({ code: 200, msg: '注册成功' });
  } catch (err) {
    return res.json({ code: 500, msg: '注册失败' });
  }
});

app.post('/login', async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) return res.json({ code: 400, msg: '请输入账号密码' });
  try {
    const [rows] = await db.execute('SELECT password,nickname FROM users WHERE username=?', [username]);
    if (!rows.length) return res.json({ code: 404, msg: '账号不存在' });
    if (rows[0].password === password) {
      loginMap.set(username, {
        nickname: rows[0].nickname || username,
        password: password
      });
      return res.json({ code: 200, msg: '登录成功' });
    } else {
      return res.json({ code: 400, msg: '密码错误' });
    }
  } catch (err) {
    return res.json({ code: 500, msg: '服务器错误' });
  }
});

// Socket.IO 正确初始化
const io = new Server(server, {
  cors: {
    origin: ["https://im6.qzz.io", "https://www.im6.qzz.io"],
    methods: ["GET", "POST"],
    credentials: true
  },
  transports: ['websocket', 'polling'],
  pingTimeout: 60000,
  pingInterval: 25000,
  maxHttpBufferSize: 10*1024*1024,
  allowEIO3: true
});

io.on('connection', socket => {
  const sid = socket.id;
  const user = {
    id: sid, socket, username: '', partner: null, isMatched: false,
    lastActive: Date.now(), lastKeepAlive: 0, roomId: null
  };
  userMap.set(sid, user);

  const timer = setInterval(() => {
    if (!user.username || !loginMap.has(user.username) || userSessionMap.get(user.username) !== sid) {
      socket.disconnect();
      userMap.delete(sid);
      clearInterval(timer);
    }
  }, UNLOGGED_CLEAN_INTERVAL);

  socket.on('clear-chat', async () => {
    if (user.username) await clearUserChatRecords(user.username);
    socket.emit('clear-chat-record', { msg: '清空成功' });
  });

  socket.on('change-nick', async (data) => {
    try {
      const { newNick } = data;
      if (!user.username) return socket.emit('nick-result', { success: false, msg: '未登录' });
      if (!newNick || newNick.length < 2 || newNick.length > 20)
        return socket.emit('nick-result', { success: false, msg: '长度2-20' });
      
      // 只改昵称，不改用户名！
      await db.execute('UPDATE users SET nickname=? WHERE username=?', [newNick, user.username]);
      const info = loginMap.get(user.username);
      info.nickname = newNick;
      loginMap.set(user.username, info);
      socket.emit('nick-result', { success: true, newNick });
    } catch (e) {
      socket.emit('nick-result', { success: false, msg: '修改失败' });
    }
  });

  socket.on('checkLogin', (data) => {
    const { userId, token } = data;
    if (!userId || !token || token !== userId || !loginMap.has(userId))
      return socket.emit('notLogin');
    user.username = userId;
    userSessionMap.set(userId, sid);
    usernameToSocket.set(userId, socket);
    pushOfflineMsg(socket, userId);
    socket.emit('loginSuccess');
    autoJoinMatchPool(sid);
  });

  socket.on('HEARTBEAT', () => {
    if (user.username && loginMap.has(user.username) && userSessionMap.get(user.username) === sid) {
      user.lastActive = Date.now();
      user.lastKeepAlive = Date.now();
      socket.emit('heartbeat-ack');
      if (user.roomId) resetRoomExpire(user.roomId);
    }
  });

  socket.on('user-online', (data) => {
    const { username } = data;
    if (!username || !loginMap.has(username)) return socket.emit('invalid-username');
    user.username = username;
    userSessionMap.set(username, sid);
    usernameToSocket.set(username, socket);
    user.lastActive = Date.now();
    socket.emit('username-set-success');
    autoJoinMatchPool(sid);
  });

  socket.on('match-chat', () => {
    if (!user.username) return socket.emit('match-end', { info: '请登录' });
    if (user.isMatched) stopChat(sid, false);
    if (waitingUsers.has(sid)) return socket.emit('match-tip', { info: '排队中' });
    waitingUsers.add(sid);
    const timer = setTimeout(() => assignAiRobot(sid), MATCH_TIMEOUT);
    userMatchTimer.set(sid, timer);
    tryMatch();
  });

  socket.on('stop-chat', () => stopChat(sid, true));
  socket.on('check-partner', () => checkPartnerStatus(sid));

  // 消息发送 —— 已修复！
  socket.on('send-msg', async (data) => {
    try {
      if (!user.username || !user.isMatched || !user.partner || !data)
        return socket.emit('msg-fail', { info: '无法发送' });
      const content = data.content || '';
      if (!content) return socket.emit('msg-fail', { info: '内容为空' });

      const to = userMap.get(user.partner);
      const toUser = to?.username || 'unknown';
      const fromNick = loginMap.get(user.username)?.nickname || user.username;

      if (db) {
        await db.execute('INSERT INTO messages (from_user,to_user,content,msg_type,msg_id) VALUES (?,?,?,?,?)',
          [user.username, toUser, content, data.type || 'text', data.msgId || '']);
      }

      if (user.partner === 'ai_bot') {
        if (data.type === 'text') {
          const reply = await callAI(content);
          setTimeout(() => {
            socket.emit('new-msg', {
              content: reply, type: 'text', burn: false,
              receipt: false, msgId: Date.now().toString(),
              fromId: 'ai_bot', fromName: 'AI陪伴者'
            });
          }, 600);
        }
        return;
      }

      if (to && to.socket) {
        to.socket.emit('new-msg', {
          content: content,
          type: data.type || 'text',
          burn: data.burn || false,
          receipt: data.receipt || false,
          msgId: data.msgId || '',
          fromId: sid,
          fromName: fromNick
        });
      } else {
        saveOfflineMsg(toUser, {
          fromId: user.username,
          content: content,
          type: data.type || 'text',
          burn: data.burn || false,
          receipt: data.receipt || false,
          msgId: data.msgId || ''
        });
      }
    } catch (e) {
      socket.emit('msg-fail', { info: '发送失败' });
    }
  });

  socket.on('msg-read-confirm', async (data) => {
    try {
      const { msgId } = data;
      if (!user.username || !msgId) return;
      if (db) await db.execute('UPDATE messages SET is_read=1 WHERE msg_id=? AND to_user=?', [msgId, user.username]);
      
      // 已读回执回传
      const partner = userMap.get(user.partner);
      if (partner && partner.socket) {
        partner.socket.emit('msg-read', { msgId });
      }
    } catch (e) {}
  });

  socket.on('RECONNECT', (data) => {
    const { userId, roomId } = data;
    if (!userId || !roomId || userId !== user.username) return socket.emit('RECONNECT_RESULT', { success: false });
    const ret = checkReconnectValid(userId, roomId);
    socket.emit('RECONNECT_RESULT', ret);
    if (ret.success) {
      user.isMatched = true;
      user.roomId = roomId;
      pushOfflineMsg(socket, userId);
    }
  });

  socket.on('LEAVE', (data) => {
    const { userId, roomId } = data;
    if (!userId || !roomId || userId !== user.username) return socket.emit('LEAVE_RESULT', { success: false });
    markUserLeave(userId, roomId);
    stopChat(sid, true);
    socket.emit('LEAVE_RESULT', { success: true });
  });

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
  });
});

// ===================== 加强版 3 分钟保活 + 自动重启 =====================
const TARGET_PORT = 10000;
const KEEPALIVE_INTERVAL = 3 * 60 * 1000;
const FAILURE_THRESHOLD = 3;
let consecutiveFailures = 0;

function keepAlive() {
  const now = new Date();
  const timeStr = now.toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai' });
  const req = http.get(`http://127.0.0.1:${TARGET_PORT}`, function(res) {
    res.resume();
    if (res.statusCode === 200) {
      consecutiveFailures = 0;
      console.log(`[✅ 保活成功] ${timeStr}`);
    } else {
      consecutiveFailures++;
      console.log(`[⚠️ 保活异常] ${timeStr} 连续失败：${consecutiveFailures}`);
    }
  });
  req.setTimeout(5000, () => {
    req.destroy();
    consecutiveFailures++;
    console.log(`[❌ 保活超时] ${timeStr} 连续失败：${consecutiveFailures}`);
  });
  req.on('error', () => {
    consecutiveFailures++;
    console.log(`[❌ 保活失败] ${timeStr} 连续失败：${consecutiveFailures}`);
  });
}

const keepAliveTimer = setInterval(keepAlive, KEEPALIVE_INTERVAL);

setInterval(() => {
  if (consecutiveFailures >= FAILURE_THRESHOLD) {
    console.log('[🚨 连续失败3次，进程自动重启]');
    clearInterval(keepAliveTimer);
    process.exit(1);
  }
}, 1000);

loadPwd();
startKeepAliveCheck();

app.use((req, res) => res.status(404).json({ code: 404, msg: '不存在' }));

server.listen(PORT, '0.0.0.0', () => {
  console.log('=========================================');
  console.log('✅ 最终修复版启动成功 | 端口：' + PORT);
  console.log('✅ 无加密、无冲突、消息秒转发');
  console.log('✅ 昵称只改昵称、用户名不动、数据库正常保存');
  console.log('✅ 跨域已加：im6.qzz.io + www.im6.qzz.io');
  console.log('✅ 加强版3分钟保活 + 挂了自动重启');
  console.log('=========================================');
  showMenu();
});
