const http = require('http');
const process = require('process');
const express = require('express');
const mysql = require('mysql2/promise');
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const axios = require('axios');
const { exec } = require('child_process');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
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
    sysLog('DB', 'MySQL 连接成功');
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
// 🔥 全局统一日志（控制台 + 文件双输出 · 与前端完全匹配）
// ==============================================
const LOG_PATH = path.join(__dirname, 'server.log');
function sysLog(tag, msg, data = {}) {
  const t = new Date().toLocaleString('zh-CN');
  let logStr = `[${t}] [${tag}] ${msg}`;
  if (Object.keys(data).length > 0) {
    logStr += ' | ' + JSON.stringify(data);
  }
  console.log(logStr);
  fs.appendFileSync(LOG_PATH, logStr + '\n', { flag: 'a' });
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
      loginMap.set(u.username, { nickname: u.nickname || u.username, password: u.password });
    });
    sysLog('USER', '用户数据加载完成', { count: rows.length });
  } catch (e) {}
}

async function clearUserChatRecords(username) {
  try {
    if (db && username) {
      await db.execute("DELETE FROM messages WHERE from_user=? OR to_user=?", [username, username]);
      sysLog('CHAT', '清空个人聊天记录', { user: username });
    }
  } catch (e) {}
}

function loadPwd() {
  try {
    ADMIN_PWD = fs.readFileSync(PWD_FILE, 'utf8').trim() || '123456';
  } catch (e) { ADMIN_PWD = '123456'; }
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
  sysLog('CHAT', '聊天结束', { user: me.username, self: isInitiative });
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
  sysLog('MATCH', '匹配AI成功', { user: u.username });
}

function autoJoinMatchPool(sid) {
  const u = userMap.get(sid);
  if (!u || !u.socket.connected || !u.username || !loginMap.has(u.username) || userSessionMap.get(u.username) !== sid || u.isMatched || waitingUsers.has(sid)) return;
  waitingUsers.add(sid);
  const timer = setTimeout(() => assignAiRobot(sid), MATCH_TIMEOUT);
  userMatchTimer.set(sid, timer);
  tryMatch();
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
        if (u) autoJoinMatchPool(uid);
        if (p) autoJoinMatchPool(pid);
        sysLog('KEEPALIVE', '对方离线，自动断开', { u: u?.username, p: p?.username });
        return;
      }

      if (now > val.expireTime) {
        stopChat(uid, false);
        stopChat(pid, false);
        keepAliveMap.delete(uid);
        keepAliveMap.delete(pid);
        sysLog('KEEPALIVE', '保活过期', { u: u.username, p: p.username });
        return;
      }
    });
  }, KEEP_ALIVE_CHECK_INTERVAL);
}

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
  res.send('✅ 服务运行正常 · 全局日志已开启');
});

// 注册
app.post('/register', async (req, res) => {
  try {
    // 先判断数据库是否就绪
    if (!db) {
      return res.json({ code: 500, msg: "数据库初始化中，请稍候重试" });
    }

    const { username, password } = req.body;
    if (!username || !password) {
      return res.json({ code: 400, msg: '请输入账号密码' });
    }

    const [rows] = await db.execute('SELECT id FROM users WHERE username=?', [username]);
    if (rows.length > 0) {
      return res.json({ code: 400, msg: '账号已存在' });
    }

    await db.execute('INSERT INTO users (username, password, nickname) VALUES (?,?,?)', [username, password, username]);
    loginMap.set(username, { nickname: username, password });
    sysLog('USER', '新用户注册', { username });

    return res.json({ code: 200, msg: '注册成功' });
  } catch (err) {
    console.error('注册异常：', err);
    return res.json({ code: 500, msg: '注册失败' });
  }
});

// 登录
app.post('/login', async (req, res) => {
  try {
    // 先判断数据库就绪没有（最关键）
    if (!db) {
      return res.json({ code: 500, msg: "数据库初始化中，请稍候重试" });
    }

    const { username, password } = req.body;
    if (!username || !password) {
      return res.json({ code: 400, msg: '请输入账号密码' });
    }

    const [rows] = await db.execute('SELECT password,nickname FROM users WHERE username=?', [username]);

    if (!rows || rows.length === 0) {
      return res.json({ code: 404, msg: '账号不存在' });
    }

    if (rows[0].password !== password) {
      return res.json({ code: 400, msg: '密码错误' });
    }

    // ✅ 修复：数组取值 rows[0]
    loginMap.set(username, {
      nickname: rows[0].nickname || username,
      password: password
    });

    sysLog('USER', '用户登录', { username });
    return res.json({ code: 200, msg: '登录成功' });

  } catch (err) {
    console.error("登录异常:", err);
    return res.json({ code: 500, msg: '服务器异常，请稍后再试' });
  }
});

// ✅ 修复：Socket.io 正确跨域配置
const io = new Server(server, {
  cors: {
    origin: ["https://im6.qzz.io", "https://www.im6.qzz.io"],
    credentials: true
  },
  transports: ['websocket','polling'], 
  pingTimeout:60000, 
  pingInterval:25000
});


io.on('connection', socket => {
  const sid = socket.id;
  const user = {
    id: sid, socket, username:'', partner:null, isMatched:false,
    lastActive:Date.now(), lastKeepAlive:0, roomId:null
  };
  userMap.set(sid, user);
  sysLog('CONNECT', '客户端连接', { sid });

  const timer = setInterval(() => {
    if (!user.username || !loginMap.has(user.username) || userSessionMap.get(user.username) !== sid) {
      socket.disconnect();
      userMap.delete(sid);
      clearInterval(timer);
      sysLog('CONNECT', '未登录超时清理', { sid });
    }
  }, UNLOGGED_CLEAN_INTERVAL);

  // 接收前端日志 → 写入全局日志
  socket.on("client-action-log", (data) => {
    sysLog('FRONT', data.action, {
      user: data.userId,
      nick: data.nickname,
      ...data.extra,
      time: data.time
    });
  });

  socket.on('user-online', (data) => {
    const { username } = data;
    if (!username || !loginMap.has(username)) return;
    user.username = username;
    userSessionMap.set(username, sid);
    usernameToSocket.set(username, socket);
    sysLog('ONLINE', '用户上线', { username, sid });
    autoJoinMatchPool(sid);
  });

  socket.on('match-chat', () => {
    if (!user.username) return;
    if (user.isMatched) stopChat(sid, false);
    if (waitingUsers.has(sid)) return;
    waitingUsers.add(sid);
    const t = setTimeout(() => assignAiRobot(sid), MATCH_TIMEOUT);
    userMatchTimer.set(sid, t);
    sysLog('MATCH', '用户发起匹配', { user: user.username });
    tryMatch();
  });

  socket.on('stop-chat', () => {
    sysLog('MATCH', '用户停止匹配', { user: user.username });
    stopChat(sid, true);
  });

  socket.on('HEARTBEAT', () => {
    if (!user.username) return;
    user.lastKeepAlive = Date.now();
    user.lastActive = Date.now();
    socket.emit('HEARTBEAT-ACK');
  });

  socket.on('clear-chat', async () => {
    if (user.username) await clearUserChatRecords(user.username);
    socket.emit('clear-chat-record');
  });

  socket.on('change-nick', async (newNick) => {
    try {
      const nick = newNick?.trim();
      if (!user.username || !nick || nick.length<2 || nick.length>20)
        return socket.emit('nick-result', { success:false, msg:'格式错误' });
      await db.execute('UPDATE users SET nickname=? WHERE username=?', [nick, user.username]);
      const info = loginMap.get(user.username);
      if (info) { info.nickname = nick; loginMap.set(user.username, info); }
      socket.emit('nick-result', { success:true, newName:nick });
      sysLog('NICK', '修改昵称', { user: user.username, new: nick });
    } catch (err) {
      socket.emit('nick-result', { success:false, msg:'修改失败' });
    }
  });

  socket.on('send-msg', async (data) => {
    try {
      if (!user.username || !user.isMatched || !user.partner) return;
      const to = userMap.get(user.partner);
      const fromNick = loginMap.get(user.username)?.nickname || user.username;

      sysLog('MSG', '消息发送', {
        from: user.username,
        to: to?.username || 'AI',
        type: data.type || 'text',
        burn: data.burn || false
      });

      if (user.partner === 'ai_bot' && data.type === 'text') {
        const reply = await callAI(data.content);
        setTimeout(() => {
          socket.emit('new-msg', {
            content:reply, type:'text', burn:false,
            msgId:Date.now().toString(), fromName:'AI陪伴者'
          });
        }, 600);
        return;
      }

      if (to && to.socket) {
        to.socket.emit('new-msg', {
          content:data.content, type:data.type||'text', burn:data.burn||false,
          msgId:data.msgId||'', fromName:fromNick
        });
      }
    } catch (err) {}
  });

  socket.on('msg-read', (data) => {
    const p = userMap.get(user.partner);
    if (p && p.socket) {
      p.socket.emit('msg-read', { msgId: data.msgId });
      sysLog('MSG', '已读回执', { from: p.username, msgId: data.msgId });
    }
  });

  socket.on('disconnect', () => {
    cleanMatchTimer(sid);
    if (user.username) {
      userSessionMap.delete(user.username);
      usernameToSocket.delete(user.username);
    }
    keepAliveMap.delete(sid);
    userMap.delete(sid);
    waitingUsers.delete(sid);
    clearInterval(timer);
    sysLog('DISCONNECT', '客户端断开', { sid, user: user.username });
  });
});

startKeepAliveCheck();
loadPwd();

// ==============================
// 3分钟自我保活 + 崩溃自动重启（优化版：首次延迟30秒）
// ==============================
const TARGET_URL = `http://127.0.0.1:${PORT}`;
const PING_INTERVAL = 3 * 60 * 1000; // 3分钟
const FIRST_DELAY = 30 * 1000;       // 首次延迟30秒
const FAILURE_THRESHOLD = 3;
const LOG_FILE = path.join(__dirname, 'keepalive.log');
const SCRIPT_PATH = __filename;

let consecutiveFailures = 0;
let isRestarting = false;

function writeLog(msg) {
  const t = new Date().toLocaleString('zh-CN');
  const logStr = `[${t}] ${msg}`;
  fs.appendFileSync(LOG_FILE, logStr + '\n', { flag: 'a' });
  console.log(logStr);
}

// 健康检查：服务 + MySQL 双检测
async function doHealthCheck() {
  if (isRestarting) return;

  try {
    // 1. 检查服务端口
    await new Promise((resolve, reject) => {
      const req = http.get(TARGET_URL, { timeout: 5000 }, (res) => {
        if (res.statusCode === 200) resolve();
        else reject(new Error('服务异常'));
      });
      req.on('error', reject);
      req.on('timeout', () => {
        req.destroy();
        reject(new Error('请求超时'));
      });
    });

    // 2. 检查MySQL
    if (db) {
      await db.execute('SELECT 1');
    }

    consecutiveFailures = 0;
    writeLog('✅ 健康检查正常：服务+MySQL在线');
  } catch (err) {
    consecutiveFailures++;
    writeLog(`⚠️ 健康检查失败 ${consecutiveFailures}/${FAILURE_THRESHOLD}：${err.message}`);

    if (consecutiveFailures >= FAILURE_THRESHOLD && !isRestarting) {
      isRestarting = true;
      writeLog('🚨 连续异常，自动重启服务');
      const child = require('child_process').exec;
      child(`node ${SCRIPT_PATH}`, () => {});
      setTimeout(() => process.exit(1), 1500);
    }
  }
}

// 首次延迟30秒执行，之后每3分钟一次
setTimeout(() => {
  doHealthCheck();
  setInterval(doHealthCheck, PING_INTERVAL);
}, FIRST_DELAY);

// 全局异常捕获
process.on('uncaughtException', (err) => {
  sysLog('ERROR', '服务崩溃', { msg: err.message });
  consecutiveFailures = FAILURE_THRESHOLD;
  doHealthCheck();
});
process.on('unhandledRejection', () => {
  consecutiveFailures = FAILURE_THRESHOLD;
  doHealthCheck();
});

// 启动
server.listen(PORT, '0.0.0.0', () => {
  console.log('=========================================');
  console.log('✅ 服务启动成功 端口:' + PORT);
  console.log('✅ 全局统一日志：server.log');
  console.log('✅ 跨域已配置：im6.qzz.io + www.im6.qzz.io');
  console.log('✅ 3分钟保活 + 崩溃自动重启');
  console.log('✅ 登录/注册/匹配/消息/已读/日志 全部正常');
  console.log('=========================================');
});
