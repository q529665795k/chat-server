const express = require('express');
const app = express();
app.use(express.json());

// ====================== 全局HTTP极致日志（全信息）======================
app.use((req, res, next) => {
  const start = Date.now();
  const clientIp = req.ip || req.connection.remoteAddress;
  
  console.log(`\n【HTTP-请求】[${new Date().toLocaleString()}] IP:${clientIp} ${req.method} ${req.path}`);
  console.log(`请求参数:`, req.body);
  console.log(`请求头:`, req.headers['user-agent']);
  
  res.on('finish', () => {
    const cost = Date.now() - start;
    console.log(`【HTTP-响应】状态码:${res.statusCode} 耗时:${cost}ms`);
  });
  next();
});

const http = require('http');
const fs = require('fs').promises;
const fsSync = require('fs');
const path = require('path');
const axios = require('axios');
const { exec } = require('child_process');
const { Server } = require('socket.io');
require('dotenv').config();
const mysql = require('mysql2/promise');

const server = http.createServer(app);
const PORT = process.env.PORT || 10000;

// 首页根路由
app.get('/', (req, res) => {
  res.send('😎 你来啦，服务稳稳在线～');
});

// ========== 后端全局日志接口 ==========
app.post('/api/log-frontend', (req, res) => {
  console.log('📥 前端全局日志：', req.body);
  res.sendStatus(200);
});

// 获取当前用户资料（昵称+账号）
app.get('/api/get_user_info', async (req, res) => {
  try {
    const userId = req.query.user_id;
    const [rows] = await pool.query('SELECT account, nick FROM users WHERE id = ?', [userId]);
    if (rows.length > 0) {
      res.json({
        code: 200,
        account: rows[0].account,
        nick: rows[0].nick || rows[0].account
      });
    } else {
      res.json({ code: 404, msg: '用户不存在' });
    }
  } catch (err) {
    res.json({ code: 500, msg: '查询失败' });
  }
});

 async function callAI(prompt) {
  try {
    const res = await axios.post(
   axios.post("https://useavnmd-mm.hf.space/api/chat"; 
async function callAI(prompt) {
  try {
    const res = await axios.post(TARGET_URL,
    {
      model: "girl",
      messages: [{ role: "user", content: prompt }],
      web_search: true,
      stream: false
    },
    {
      timeout: 20000,
      headers: { "Content-Type": "application/json" }
    });
    return res.data.message.content || "我在呢～";
  } catch (e) {
    console.log("AI对接失败: ", e.message);
    return "AI暂时离线，稍后再试";
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
      content TEXT COMMENT '文本内容 或 文件URL',
      msg_type VARCHAR(20) DEFAULT 'text' COMMENT '消息类型:text/image/video',
      file_name VARCHAR(100) COMMENT '原文件名',
      file_size INT COMMENT '文件大小(字节)',
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
const HEARTBEAT_INTERVAL = 300000;
const HEARTBEAT_TIMEOUT = 3600000;

// 全局统一日志
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
  if (!u || !u.socket.connected || !u.username || !loginMap.has(u.username) || userSessionMap.get(u.username) !== u.id || u.isMatched || waitingUsers.has(sid)) return;
  waitingUsers.add(sid);
  const timer = setTimeout(() => assignAiRobot(sid), MATCH_TIMEOUT);
  userMatchTimer.set(sid, timer);
  tryMatch();
}

function tryMatch() {
  const list = Array.from(waitingUsers)
    .map(id => userMap.get(id))
    .filter(u => u && u.socket.connected && !u.partner && u.username && loginMap.has(u.username) && userSessionMap.get(u.username) === u.id);
  if (list.length < 2) return;
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
  const origin = req.headers.origin || "";
  if (allowOrigins.includes(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
  }
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With");
  res.setHeader("Access-Control-Allow-Credentials", "true");
  res.setHeader("Access-Control-Max-Age", "86400");
  if (req.method === "OPTIONS") {
    return res.sendStatus(200);
  }
  next();
});

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// ====================== 注册 ======================
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

// ====================== 登录 ======================
app.post('/login', async (req, res) => {
  try {
    const { username, password } = req.body;
    console.log('🔍 收到登录请求', { 输入账号: username, 输入密码: password });
    const [userRows] = await pool.query(
      `SELECT username, password, nickname FROM users WHERE username = ?`, 
      [username]
    );
    console.log('📊 数据库查询结果', { 查询账号: username, 匹配到用户数量: userRows.length });
    if (userRows.length === 0) {
      console.log('❌ 登录失败：账号不存在', { 尝试账号: username });
      return res.json({ code: 400, msg: '账号不存在' });
    }
    const user = userRows[0];
    console.log('✅ 找到用户数据', {
      数据库账号: user.username,
      数据库密码: user.password,
      数据库昵称: user.nickname || '无昵称'
    });
    if (user.password !== password) {
      console.log('❌ 登录失败：密码错误', {
        账号: username,
        前端输入密码: password,
        数据库正确密码: user.password
      });
      return res.json({ code: 400, msg: '密码错误' });
    }
    await pool.query(`INSERT INTO login_logs (username, login_time) VALUES (?, NOW())`, [username]);
    loginMap.set(username, { 
      nickname: user.nickname, 
      password: user.password 
    });
    console.log('🎉 登录完全成功', {
      登录账号: username,
      最终返回昵称: user.nickname || '无昵称，显示用户名'
    });
    res.json({ 
      code: 200, 
      msg: '登录成功', 
      data: { 
        username: user.username, 
        nickname: user.nickname 
      } 
    });
    sysLog('USER', '登录成功', { username });
  } catch (err) {
    console.error('💥 登录接口服务器错误：', err);
    res.json({ code: 500, msg: '服务器错误，登录失败' });
  }
});

// ====================== 修改昵称 ======================
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

// ====================== Socket.io ======================
const io = new Server(server, {
  cors: {
    origin: allowOrigins,
    methods: ["GET", "POST"],
    allowedHeaders: ["Content-Type", "Authorization"],
    credentials: true
  }
});

io.on('connection', socket => {
  const clientIp = socket.handshake.address;
  console.log(`\n【IO-新客户端连接】[${new Date().toLocaleString()}] IP:${clientIp} 客户端ID:${socket.id}`);
  
  socket.onAny((eventName, ...args) => {
    console.log(`【IO-收到前端事件】[${new Date().toLocaleString()}] IP:${clientIp} 事件:${eventName} 数据:`, args);
  });
  
  const oldEmit = socket.emit;
  socket.emit = function(eventName, ...args) {
    console.log(`【IO-后端发送事件】[${new Date().toLocaleString()}] IP:${clientIp} 事件:${eventName} 数据:`, args);
    return oldEmit.call(this, eventName, ...args);
  };
  
  socket.on('disconnect', (reason) => {
    console.log(`【IO-客户端断开】[${new Date().toLocaleString()}] IP:${clientIp} 客户端ID:${socket.id} 原因:${reason}`);
  });

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

  // 发消息：只转发链接，不处理文件
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
        to.socket.emit('new-msg', {
          content: data.content,
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
// ========== 原有：A机自心跳（负责：A机挂了就重启） ==========
const SELF_URL = `http://127.0.0.1:${PORT}`; // 只ping自己A机
const PING_INTERVAL = 3 * 60 * 1000;
const FIRST_DELAY = 30 * 1000;
const FAILURE_THRESHOLD = 3;
const LOG_FILE = path.join(__dirname, 'keepalive.log');
const SCRIPT_PATH = __filename;
let consecutiveFailures = 0;
let isRestarting = false;

// ========== 新增：B机 Ollama 心跳（负责：保B机，挂了只记录不重启） ==========
const OLLAMA_URL = `https://useavnmd-mm.hf.space/api/tags`;
let ollamaFailures = 0;

async function writeLog(msg) {
  const t = new Date().toLocaleString('zh-CN');
  const logStr = `[${t}] ${msg}\n`;
  try {
    await fs.appendFile(LOG_FILE, logStr, 'utf8');
  } catch (e) {}
  console.log(logStr.trim());
}

// ========== 原有：ping自己A机 ==========
function pingSelf() {
  return new Promise((resolve, reject) => {
    const req = http.get(SELF_URL, { timeout: 60000 }, (res) => {
      if (res.statusCode === 200) resolve();
      else reject();
    });
    req.on('error', reject);
    req.end();
  });
}

// ========== 新增：只pingB机，失败不重启 ==========
function pingOllama() {
  return new Promise((resolve, reject) => {
    const req = http.get(OLLAMA_URL, { timeout: 60000 }, (res) => {
      if (res.statusCode === 200) resolve();
      else reject();
    });
    req.on('error', reject);
    req.end();
  });
}

// ========== 原有：A机自心跳循环（失败重启） ==========
async function selfKeepAlive() {
  if (isRestarting) return;
  try {
    await pingSelf();
    consecutiveFailures = 0;
  } catch (err) {
    consecutiveFailures++;
    await writeLog(`⚠️ A机心跳失败，连续${consecutiveFailures}次`);
    if (consecutiveFailures >= FAILURE_THRESHOLD) {
      isRestarting = true;
      await writeLog(`🚨 A机心跳崩了，重启聊天后端！`);
      process.exit(1);
    }
  }
}

// ========== 新增：B机心跳循环（只记录，绝不重启） ==========
async function ollamaKeepAlive() {
  try {
    await pingOllama();
    ollamaFailures = 0;
  } catch (err) {
    ollamaFailures++;
    await writeLog(`⚠️ B机Ollama心跳异常，连续${ollamaFailures}次`);
  }
}

// ========== 启动两个独立心跳 ==========
async function startKeepAliveCheck() {
  // A机自心跳
  setTimeout(() => {
    selfKeepAlive();
    setInterval(selfKeepAlive, PING_INTERVAL);
  }, FIRST_DELAY);

  // B机保活心跳（和A机同频3分钟一次）
  setTimeout(() => {
    ollamaKeepAlive();
    setInterval(ollamaKeepAlive, PING_INTERVAL);
  }, FIRST_DELAY);

  await writeLog(`✅ 双心跳启动：A机自检+ B机Ollama保活`);
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

// 数据库保活（仅修改了轮询时间和日志文字，其余完全不动）
const DB_HEARTBEAT_INTERVAL = 900000; // 原来的18000000改成900000，即15分钟
setTimeout(async () => {
  try {
    await pool.query("SELECT 1");
    console.log("✅【首次检测】数据库运行正常");
  } catch (err) {
    console.error("⚠️【首次检测】数据库异常：", err.message);
  }
  setInterval(async () => {
    try {
      await pool.query("SELECT 1");
      console.log("✅【15分钟轮询】数据库正常"); // 把“5小时”改成“15分钟”
    } catch (err) {
      console.error("⚠️【15分钟轮询】数据库异常：", err.message); // 同上修改
    }
  }, DB_HEARTBEAT_INTERVAL);
}, 40 * 1000);

setTimeout(() => {
  doHealthCheck();
  setInterval(doHealthCheck, PING_INTERVAL);
}, FIRST_DELAY);


// 启动服务

// ====================== 监控面板 纯净同源版 开始 ======================


// 保留原有public静态托管不变
app.use(express.static(path.join(__dirname, 'public')));

// 放行根目录html（你的监控.html）
app.use(express.static(__dirname, {
  index: false,
  extensions: ['html']
}));

// 获取服务信息接口
app.get("/api/serverInfo", (req, res) => {
  res.json({
    port: PORT,
    serverTime: new Date().toLocaleString()
  });
});

// 在线用户列表接口
app.get("/api/onlineUser", (req, res) => {
  const onlineList = [];
  userMap.forEach(item => {
    if (item.username && loginMap.has(item.username)) {
      const info = loginMap.get(item.username);
      onlineList.push({
        username: item.username,
        nickname: info.nickname || item.username,
        isMatched: item.isMatched ? "已匹配" : "空闲中"
      });
    }
  });
  res.json({
    code: 200,
    total: onlineList.length,
    list: onlineList
  });
});

// 清空聊天记录接口
app.post("/api/clearChatOnly", async (req, res) => {
  try {
    await pool.query("TRUNCATE TABLE messages");
    offlineMsgMem.clear?.();
    res.json({ code: 200, msg: "聊天记录已清空，匹配/在线不受影响" });
    sysLog?.("ADMIN","后台清空聊天记录");
  } catch (err) {
    res.json({ code: 500, msg: "清空失败" });
  }
});
// ====================== 监控面板 结束 ======================

server.listen(PORT, '0.0.0.0', () => {
  console.log('=========================================');
  console.log('✅ 服务启动成功 端口:' + PORT);
  console.log('✅ 已删除：本地上传、multer、文件存储、假链接');
  console.log('✅ 图片/视频只接收Worker返回的真实URL，直接转发');
  console.log('✅ 全程无服务器存储、无带宽压力');
  console.log('=========================================');
});
