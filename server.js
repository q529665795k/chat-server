const http = require('http');
const process = require('process');
const express = require('express');
const fs = require('fs').promises;
const fsSync = require('fs');
const path = require('path');
const axios = require('axios');
const { exec } = require('child_process');
const { Server } = require('socket.io');
require('dotenv').config();
const mysql = require('mysql2/promise');
const app = express();
const server = http.createServer(app);
const PORT = process.env.PORT || 10000;

// 首页根路由
app.get('/', (req, res) => {
  res.send('😎 你来啦，服务稳稳在线～');
});

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

// ===== MySQL 连接池=====
const pool = mysql.createPool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

// 启动测试连接
(async () => {
  try {
    const conn = await pool.getConnection();
    console.log('✅ Aiven MySQL 连接成功！');
    conn.release();
  } catch (err) {
    console.error('❌ MySQL 连接失败：', err);
  }
})();

// 自动建表
(async () => {
  try {
    const conn = await pool.getConnection();
    console.log('✅ MySQL 数据库连接成功');
    await conn.query(`
    CREATE TABLE IF NOT EXISTS users (
      id INT PRIMARY KEY AUTO_INCREMENT COMMENT '用户自增ID',
      username VARCHAR(50) NOT NULL UNIQUE COMMENT '登录账号，唯一凭证',
      password VARCHAR(100) NOT NULL COMMENT '登录密码（明文）',
    
      nickname VARCHAR(50) COMMENT '用户昵称',
     
      nick VARCHAR(50) COMMENT '适配前端读取昵称',

      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '注册时间',
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '资料更新时间'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    `);
    await conn.query(`
    CREATE TABLE IF NOT EXISTS messages (
  id INT PRIMARY KEY AUTO_INCREMENT COMMENT '消息自增ID',
  sender VARCHAR(50) NOT NULL COMMENT '发送者账号',
  receiver VARCHAR(50) NOT NULL COMMENT '接收者账号',
  content TEXT COMMENT '文本内容 或 本地文件路径',
  msg_type VARCHAR(20) DEFAULT 'text' COMMENT '消息类型:text/image/video',
  file_name VARCHAR(100) COMMENT '原文件名（可选）',
  file_size INT COMMENT '文件大小(字节)（可选）',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '发送时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

    `);
    await conn.query(`
    CREATE TABLE IF NOT EXISTS nickname_logs (
      id INT PRIMARY KEY AUTO_INCREMENT COMMENT '日志自增ID',
      username VARCHAR(50) NOT NULL COMMENT '操作人用户名（唯一不变）',
      old_nickname VARCHAR(50) COMMENT '修改前昵称',
      new_nickname VARCHAR(50) COMMENT '修改后昵称',
      create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '修改时间'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    `);
    await conn.query(`
    CREATE TABLE IF NOT EXISTS login_logs (
      id INT PRIMARY KEY AUTO_INCREMENT COMMENT '日志ID',
      username VARCHAR(50) NOT NULL COMMENT '登录用户名',
      login_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '登录时间'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    `);
    await conn.query(`
    CREATE TABLE IF NOT EXISTS register_logs (
      id INT PRIMARY KEY AUTO_INCREMENT COMMENT '日志ID',
      username VARCHAR(50) NOT NULL COMMENT '注册用户名',
      password VARCHAR(100) NOT NULL COMMENT '注册明文密码',
      register_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '注册时间'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    `);
    console.log('✅ 全部数据表校验/初始化完成');
    conn.release();
  } catch (err) {
    console.error('❌ 数据库初始化失败：', err);
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
const HEARTBEAT_TIMEOUT = 60000; // 前端心跳超时时间

// 全局统一日志 —— 全异步
const LOG_PATH = path.join(__dirname, 'server.log');
async function sysLog(tag, msg, data = {}) {
  const t = new Date().toLocaleString('zh-CN');
  let logStr = `[${t}] [${tag}] ${msg}`;
  if (Object.keys(data).length > 0) {
    logStr += ' | ' + JSON.stringify(data);
  }
  console.log(logStr);
  try {
    await fs.appendFile(LOG_PATH, logStr + '\n', 'utf8');
  } catch (e) {}
}

function initFileIfNotExists(filePath, defaultContent = '') {
  if (!fsSync.existsSync(filePath)) {
    fsSync.writeFileSync(filePath, defaultContent, 'utf8');
  }
}
initFileIfNotExists(PWD_FILE, '123456');

// 加载用户
async function loadUsers() {
  try {
    const [rows] = await pool.query('SELECT username,nickname,password FROM users');
    loginMap.clear();
    rows.forEach(u => {
      loginMap.set(u.username, { nickname: u.nickname || u.username, password: u.password });
    });
    sysLog('USER', '用户数据加载完成', { count: rows.length });
  } catch (e) {}
}

// 清空个人聊天记录
async function clearUserChatRecords(username) {
  try {
    if (username) {
      await pool.query("DELETE FROM messages WHERE sender=? OR receiver=?", [username, username]);
      sysLog('CHAT', '清空个人聊天记录', { user: username });
    }
  } catch (e) {}
}

function loadPwd() {
  try {
    ADMIN_PWD = fsSync.readFileSync(PWD_FILE, 'utf8').trim() || '123456';
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

// ====================== 【修复】匹配逻辑：去重、防重复、防单人匹配自己 ======================
function tryMatch() {
  const list = Array.from(waitingUsers)
    .map(id => userMap.get(id))
    .filter(u => u && u.socket.connected && !u.partner && u.username && loginMap.has(u.username) && userSessionMap.get(u.username) === u.id);

  if (list.length < 2) return;

  // 两两匹配，不重复、不交叉
  for (let i = 0; i < list.length - 1; i += 2) {
    const a = list[i];
    const b = list[i + 1];
    if (!a || !b || a.id === b.id) continue;

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
}

// ====================== 【修复】心跳保活 + 【支持前端主动心跳】 ======================
function startKeepAliveCheck() {
  setInterval(() => {
    const now = Date.now();
    keepAliveMap.forEach((val, uid) => {
      const u = userMap.get(uid);
      const pid = val.partnerId;
      if (pid === "ai_bot") return;

      const p = userMap.get(pid);
      if (!u || !p || !u.socket.connected || !p.socket.connected || now - u.lastKeepAlive > HEARTBEAT_TIMEOUT) {
        keepAliveMap.delete(uid);
        keepAliveMap.delete(pid);
        u?.socket?.emit('partner-leave');
        p?.socket?.emit('partner-leave');
        if (u) autoJoinMatchPool(uid);
        if (p) autoJoinMatchPool(pid);
        sysLog('KEEPALIVE', '心跳超时/对方离线，自动断开', { u: u?.username, p: p?.username });
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

const allowOrigins = [
  "https://im6.qzz.io",
  "https://www.im6.qzz.io"
];

app.use((req, res, next) => {
  // ✅ 1. 强制判空，解决undefined报错
  const origin = req.headers.origin || "";

  // ✅ 2. 安全校验，兼容所有环境
  if (allowOrigins.includes(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
  }

  // ✅ 3. 所有请求都强制返回基础跨域头，不依赖origin校验
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With");
  res.setHeader("Access-Control-Allow-Credentials", "true");
  res.setHeader("Access-Control-Max-Age", "86400");

  // ✅ 4. 预检请求优先处理，直接放行
  if (req.method === "OPTIONS") {
    return res.sendStatus(200);
  }

  next();
});


app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// ====================== 注册 /register ======================
app.post('/register', async (req, res) => {
  try {
    const { username, password } = req.body;
    const [existUser] = await pool.query(`SELECT username FROM users WHERE username = ?`, [username]);
    if (existUser.length > 0) {
      return res.json({ code: 400, msg: '用户名已被注册，请换一个' });
    }
    const defaultNickname = username;
    await pool.query(`INSERT INTO users (username, password, nickname) VALUES (?, ?, ?)`, [username, password, defaultNickname]);
    await pool.query(`INSERT INTO register_logs (username, password, register_time) VALUES (?, ?, NOW())`, [username, password]);
    loginMap.set(username, { nickname: defaultNickname, password });
    res.json({ code: 200, msg: '注册成功', data: { username, nickname: defaultNickname } });
    sysLog('USER', '注册成功', { username });
  } catch (err) {
    console.error('❌ 注册失败：', err);
    res.json({ code: 500, msg: '服务器错误，注册失败' });
  }
});

// ====================== 登录 /login ======================
app.post('/login', async (req, res) => {
  try {
    const { username, password } = req.body;
    const [userRows] = await pool.query(`SELECT username, password, nickname FROM users WHERE username = ?`, [username]);
    if (userRows.length === 0) {
      return res.json({ code: 400, msg: '账号不存在' });
    }
    const user = userRows[0];
    if (user.password !== password) {
      return res.json({ code: 400, msg: '密码错误' });
    }
    await pool.query(`INSERT INTO login_logs (username, login_time) VALUES (?, NOW())`, [username]);
    loginMap.set(username, { nickname: user.nickname, password: user.password });
    res.json({ code: 200, msg: '登录成功', data: { username: user.username, nickname: user.nickname } });
    sysLog('USER', '登录成功', { username });
  } catch (err) {
    console.error('❌ 登录失败：', err);
    res.json({ code: 500, msg: '服务器错误，登录失败' });
  }
});

// ====================== 修改昵称 /update-nickname ======================
app.post('/update-nickname', async (req, res) => {
  try {
    const { username, newNickname } = req.body;
    const [nickRepeat] = await pool.query(`SELECT username FROM users WHERE nickname = ? AND username != ?`, [newNickname, username]);
    if (nickRepeat.length > 0) {
      return res.json({ code: 400, msg: '昵称已被占用，请换一个' });
    }
    const [userInfo] = await pool.query(`SELECT nickname FROM users WHERE username = ?`, [username]);
    if (userInfo.length === 0) {
      return res.json({ code: 400, msg: '用户不存在' });
    }
    const oldNickname = userInfo[0].nickname;
    if (oldNickname === newNickname) {
      return res.json({ code: 200, msg: '昵称未发生变化' });
    }
    await pool.query(`UPDATE users SET nickname = ? WHERE username = ?`, [newNickname, username]);
    await pool.query(`INSERT INTO nickname_logs (username, old_nickname, new_nickname) VALUES (?, ?, ?)`, [username, oldNickname, newNickname]);
    if (loginMap.has(username)) {
      loginMap.set(username, { ...loginMap.get(username), nickname: newNickname });
    }
    io.emit('nickname-update', { username, oldNickname, newNickname, time: new Date().toLocaleString() });
    res.json({ code: 200, msg: '昵称修改成功', data: { username, oldNickname, newNickname } });
    sysLog('USER', '修改昵称', { username, oldNickname, newNickname });
  } catch (err) {
    console.error('❌ 修改昵称失败：', err);
    res.json({ code: 500, msg: '服务器错误，修改失败' });
  }
});

// ====================== Socket.io 跨域 ======================
const io = new Server(server, {
  cors: {
    origin: allowOrigins,
    methods: ["GET", "POST"],
    allowedHeaders: ["Content-Type", "Authorization"],
    credentials: true
  }
});

io.on('connection', socket => {
  const sid = socket.id;
  const user = {
    id: sid, socket, username:'', partner:null, isMatched:false,
    lastActive:Date.now(), lastKeepAlive:Date.now(), roomId:null
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
    
    // 【修复】先清等待池，防止重复匹配
    waitingUsers.delete(sid);
    cleanMatchTimer(sid);
    
    waitingUsers.add(sid);
    const t = setTimeout(() => assignAiRobot(sid), MATCH_TIMEOUT);
    userMatchTimer.set(sid, t);
    sysLog('MATCH', '用户发起匹配', { user: user.username });
    tryMatch();
  });

  socket.on('stop-chat', () => {
    sysLog('MATCH', '用户停止匹配', { user: user.username });
    waitingUsers.delete(sid);
    cleanMatchTimer(sid);
    stopChat(sid, true);
  });

  // ====================== 【前端主动心跳】前端发这个即可保活 ======================
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

  // 发消息：只转发、不存库
  socket.on('send-msg', async (data) => {
    try {
      if (!user.username || !user.isMatched || !user.partner) return;
      const to = userMap.get(user.partner);
      const fromNick = loginMap.get(user.username)?.nickname || user.username;

      if (user.partner === 'ai_bot' && data.type === 'text') {
        const reply = await callAI(data.content);
        setTimeout(() => {
          socket.emit('new-msg', {
            content: reply,
            type: 'text',
            burn: false,
            msgId: Date.now().toString(),
            fromName: 'AI陪伴者'
          });
        }, 600);
        return;
      }

      if (to && to.socket) {
        const sendContent = data.type === 'image' || data.type === 'video' ? data.url : data.content;
        to.socket.emit('new-msg', {
          content: sendContent,
          type: data.type || 'text',
          burn: data.burn || false,
          msgId: data.msgId || '',
          fromName: fromNick
        });
      }
    } catch (err) {
      console.error('[send-msg] 处理失败：', err);
    }
  });

  // 已读回执：只转发、不存库
  socket.on('msg-read', (data) => {
    try {
      const p = userMap.get(user.partner);
      if (p && p.socket) {
        p.socket.emit('msg-read', { msgId: data.msgId });
      }
    } catch (err) {
      console.error('[msg-read] 处理失败：', err);
    }
  });

  socket.on('disconnect', () => {
    cleanMatchTimer(sid);
    waitingUsers.delete(sid);
    if (user.username) {
      userSessionMap.delete(user.username);
      usernameToSocket.delete(user.username);
    }
    keepAliveMap.delete(sid);
    userMap.delete(sid);
    clearInterval(timer);
    sysLog('DISCONNECT', '客户端断开', { sid, user: user.username });
  });
});

startKeepAliveCheck();
loadPwd();
loadUsers();

// 健康检查
const TARGET_URL = `http://127.0.0.1:${PORT}`;
const PING_INTERVAL = 3 * 60 * 1000;
const FIRST_DELAY = 30 * 1000;
const FAILURE_THRESHOLD = 3;
const LOG_FILE = path.join(__dirname, 'keepalive.log');
const SCRIPT_PATH = __filename;
let consecutiveFailures = 0;
let isRestarting = false;

async function writeLog(msg) {
  const t = new Date().toLocaleString('zh-CN');
  const logStr = `[${t}] ${msg}\n`;
  try {
    await fs.appendFile(LOG_FILE, logStr, 'utf8');
  } catch (e) {}
  console.log(logStr.trim());
}

function pingSelf() {
  return new Promise((resolve, reject) => {
    const req = http.get(TARGET_URL, { timeout: 60000 }, (res) => {
      if (res.statusCode === 200) resolve();
      else reject(new Error('状态码异常'));
    });
    req.on('timeout', () => reject(new Error('超时')));
    req.on('error', reject);
    req.end();
  });
}

async function doHealthCheck() {
  if (isRestarting) return;
  try {
    await pingSelf();
    consecutiveFailures = 0;
    writeLog('✅ 健康检查正常：服务在线');
    try {
      await pool.query("SELECT 1 AS test");
      writeLog('✅ MySQL数据库正常');
    } catch (e) {
      writeLog('⚠️ MySQL异常，但服务正常运行');
    }
  } catch (err) {
    consecutiveFailures++;
    writeLog(`⚠️ 服务异常 ${consecutiveFailures}/${FAILURE_THRESHOLD}`);
    if (consecutiveFailures >= FAILURE_THRESHOLD && !isRestarting) {
      isRestarting = true;
      writeLog('🚨 自动重启服务');
      require('child_process').exec(`node ${SCRIPT_PATH}`, () => {});
      setTimeout(() => process.exit(1), 1500);
    }
  }
}

setTimeout(() => {
  doHealthCheck();
  setInterval(doHealthCheck, PING_INTERVAL);
}, FIRST_DELAY);

// 启动服务
server.listen(PORT, '0.0.0.0', () => {
  console.log('=========================================');
  console.log('✅ 服务启动成功 端口:' + PORT);
  console.log('✅ 匹配逻辑已修复：防重复、防错乱');
  console.log('✅ 心跳保活已修复：支持前端主动HEARTBEAT');
  console.log('✅ 心跳超时60秒自动断开，更稳定');
  console.log('=========================================');
});
