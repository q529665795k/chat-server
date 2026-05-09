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
    const [rows] = await pool.query(
      'SELECT username, nickname FROM users WHERE username = ?', 
      [userId]
    );

    if (rows.length > 0) {
      res.json({
        code: 200,
        account: rows[0].username,
        nick: rows[0].nickname || rows[0].username
      });
      sysLog('API','获取用户信息成功',{userId});
    } else {
      res.json({ code: 404, msg: '用户不存在' });
      sysLog('API','获取用户信息失败：用户不存在',{userId});
    }
  } catch (err) {
    console.error('获取用户信息接口错误：',err);
    sysLog('ERROR','获取用户信息接口异常',{err:err.message});
    res.json({ code: 500, msg: '查询失败' });
  }
});

// ====================== AI调用 ======================
async function callAI(prompt) {
  try {
    const res = await axios.post("https://api.moyu.chat/api/ai", {
      model: "qwen2:0.5b",
      messages: [{ role: "user", content: prompt }],
      stream: false
    }, {
      timeout: 15000,
      headers: { "Content-Type": "application/json" }
    });
    return res.data.message?.content || "爸爸～在呢😘";
  } catch (e) {
    console.log("AI对接失败: ", e.message);
    return "爸爸～我掉线啦🥺";
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
      id INT PRIMARY KEY AUTO_INCREMENT COMMENT '注册日志ID',
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
let userOnlineSid = new Map();
let keepAliveMap = new Map();
let userMatchTimer = new Map();
let roomMem = new Map();
let offlineMsgMem = new Map();
let usernameToSocket = new Map();

const IDLE_TIMEOUT = 3600000;
const IDLE_CHECK_INTERVAL = 60000;

// ========== 修复1：在线人数统计正确 ==========
function getOnlineCount() {
  return userOnlineSid.size;
}

// ========== 修复2：广播格式与前端完全一致 ==========
function broadcastOnlineCount() {
  io.emit('online_update', { count: getOnlineCount() });
}

const KEEP_ALIVE_EXPIRE = 24 * 60 * 60 * 1000;
const KEEP_ALIVE_CHECK_INTERVAL = 60 * 1000;
const UNLOGGED_CLEAN_INTERVAL = 300000;
const REDIS_EXPIRE = 7200;
const MATCH_TIMEOUT = 3000; // 修复：3秒必进AI
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

// 强制踢下线
function kickUserOffline(username, reason = "账号已在其他设备登录，已强制下线") {
  const oldSid = userOnlineSid.get(username);
  const oldUser = userMap.get(oldSid);
  if (oldUser && oldUser.socket) {
    oldUser.socket.emit('force-logout', { reason });
    oldUser.socket.disconnect();
  }
  userMap.delete(oldSid);
  usernameToSocket.delete(username);
  userOnlineSid.delete(username);
  broadcastOnlineCount();
  sysLog('KICK', '多端互踢强制下线', { username, reason });
}

// 断开聊天
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

// ========== 修复4：AI匹配强制成功 ==========
function assignAiRobot(sid) {
  const u = userMap.get(sid);
  if (!u || !u.socket) return;
  cleanMatchTimer(sid);
  waitingUsers.delete(sid);
  u.isMatched = true;
  u.partner = "ai_bot";
  u.roomId = "ai_room";
  u.socket.emit('match-found', {
    partnerId: "ai_bot",
    partnerName: "AI陪伴者",
    roomId: "ai_room"
  });
  sysLog('MATCH', 'AI匹配成功', { user: u.username });
}

function autoJoinMatchPool(sid) {
  const u = userMap.get(sid);
  if (!u || !u.username || u.isMatched) return;
  waitingUsers.add(sid);
  const timer = setTimeout(() => {
    assignAiRobot(sid);
  }, MATCH_TIMEOUT);
  userMatchTimer.set(sid, timer);
  tryMatch();
}

function tryMatch() {
  const list = Array.from(waitingUsers).map(id => userMap.get(id)).filter(Boolean);
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
    a.socket.emit('match-found', { partnerId: b.id, partnerName: bNick, roomId: rid });
    b.socket.emit('match-found', { partnerId: a.id, partnerName: aNick, roomId: rid });
  }
}

// 闲置检测
function startIdleCheck() {
  setInterval(() => {
    const now = Date.now();
    userMap.forEach((u, sid) => {
      if (!u.username) return;
      if (now - u.lastActive > IDLE_TIMEOUT) {
        sysLog('IDLE', '用户1小时无操作，强制下线', { username: u.username });
        kickUserOffline(u.username, "已闲置1小时无操作，系统自动下线");
      }
    });
  }, IDLE_CHECK_INTERVAL);
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
        return;
      }
      if (now > val.expireTime) {
        stopChat(uid, false);
        stopChat(pid, false);
        keepAliveMap.delete(uid);
        keepAliveMap.delete(pid);
        return;
      }
    });
  }, KEEP_ALIVE_CHECK_INTERVAL);
}

const allowOrigins = ["https://im6.qzz.io", "https://im6.ct.ws"];
app.use((req, res, next) => {
  const origin = req.headers.origin || "";
  if (allowOrigins.includes(origin)) res.setHeader("Access-Control-Allow-Origin", origin);
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  res.setHeader("Access-Control-Allow-Credentials", "true");
  if (req.method === "OPTIONS") return res.sendStatus(200);
  next();
});

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// 注册
app.post('/register', async (req, res) => {
  try {
    const { username, password } = req.body;
    const [existRows] = await pool.query(`SELECT username FROM users WHERE username = ?`, [username]);
    if (existRows.length > 0) return res.json({ code: 400, msg: '账号已存在' });
    await pool.query(`INSERT INTO users (username, password, nickname, nick) VALUES (?,?,?,?)`, [username, password, username, username]);
    await pool.query(`INSERT INTO register_logs (username, password) VALUES (?,?)`, [username, password]);
    res.json({ code: 200, msg: '注册成功' });
  } catch (err) { res.json({ code: 500, msg: '服务器错误' }); }
});

// 登录
app.post('/login', async (req, res) => {
  try {
    const { username, password } = req.body;
    const [userRows] = await pool.query(`SELECT username,password,nickname FROM users WHERE username=?`, [username]);
    if (userRows.length === 0) return res.json({ code: 400, msg: '账号不存在' });
    const user = userRows[0];
    if (user.password !== password) return res.json({ code: 400, msg: '密码错误' });
    if (userOnlineSid.has(username)) kickUserOffline(username);
    loginMap.set(username, { nickname: user.nickname });
    await pool.query(`INSERT INTO login_logs (username) VALUES (?)`, [username]);
    res.json({ code: 200, msg: '登录成功', data: { username: user.username, nickname: user.nickname } });
  } catch (err) { res.json({ code: 500, msg: '服务器错误' }); }
});

// 修改昵称
app.post('/update-nickname', async (req, res) => {
  try {
    const { username, newNickname } = req.body;
    await pool.query(`UPDATE users SET nickname=?, nick=? WHERE username=?`, [newNickname, newNickname, username]);
    await pool.query(`INSERT INTO nickname_logs (username,old_nickname,new_nickname) VALUES (?,?,?)`, [username, '', newNickname]);
    if (loginMap.has(username)) loginMap.get(username).nickname = newNickname;
    io.emit('nickname-update', { username, newNickname });
    res.json({ code: 200, msg: '修改成功' });
  } catch (err) { res.json({ code: 500, msg: '失败' }); }
});

// ====================== Socket.io 核心修复 ======================
const io = new Server(server, {
  cors: { origin: allowOrigins, methods: ["GET","POST"], credentials: true }
});

io.on('connection', (socket) => {
  const sid = socket.id;
  const user = {
    id: sid, socket, username:'', partner:null, isMatched:false,
    lastActive:Date.now(), lastKeepAlive:Date.now(), roomId:null
  };
  userMap.set(sid, user);

  const timer = setInterval(() => {
    if (!user.username) { socket.disconnect(); userMap.delete(sid); clearInterval(timer); }
  }, UNLOGGED_CLEAN_INTERVAL);

  // ========== 修复5：用户上线 → 广播进场 + 在线人数正确 ==========
  socket.on('user-online', (data) => {
    const { username } = data;
    if (!username) return;
    if (userOnlineSid.has(username)) kickUserOffline(username);
    user.username = username;
    userOnlineSid.set(username, sid);
    usernameToSocket.set(username, socket);

    // 全局进场广播
    const showName = loginMap.get(username)?.nickname || username;
    io.emit('system_tip', { text: `${showName} 进入了摸鱼基地` });
    
    broadcastOnlineCount();
    autoJoinMatchPool(sid);
  });

  socket.on('i_am_back', () => {
    if (user.username) { user.lastActive = Date.now(); user.lastKeepAlive = Date.now(); }
  });

  // ========== 修复6：匹配按钮必进AI ==========
  socket.on('match-chat', () => {
    if (!user.username) return;
    if (user.isMatched) stopChat(sid);
    waitingUsers.delete(sid);
    cleanMatchTimer(sid);
    waitingUsers.add(sid);
    setTimeout(() => assignAiRobot(sid), 1000);
    tryMatch();
  });

  socket.on('match_reset', () => {
    waitingUsers.delete(sid);
    cleanMatchTimer(sid);
  });

  socket.on('enter_ai_round_room', () => {
    if (!user.username) return;
    user.isMatched = true;
    user.partner = "ai_bot";
    user.roomId = "ai_room";
  });

  socket.on('exit_ai_round_room', () => {
    user.isMatched = false;
    user.partner = null;
    user.roomId = null;
  });

  socket.on('stop-chat', () => {
    waitingUsers.delete(sid);
    cleanMatchTimer(sid);
    stopChat(sid, true);
  });

  socket.on('HEARTBEAT', () => {
    if (user.username) { user.lastActive = Date.now(); socket.emit('HEARTBEAT-ACK'); }
  });

  socket.on('clear-chat', async () => {
    if (user.username) await clearUserChatRecords(user.username);
    socket.emit('clear-chat-record');
  });

  socket.on('send-msg', async (data) => {
    try {
      if (!user.username || !user.partner) return;
      const to = userMap.get(user.partner);
      const fromNick = loginMap.get(user.username)?.nickname || user.username;
      if (user.partner === 'ai_bot') {
        const reply = await callAI(data.content);
        setTimeout(() => {
          socket.emit('new-msg', {
            content: reply, fromName: 'AI陪伴者', type: 'text'
          });
        }, 600);
        return;
      }
      if (to && to.socket) {
        to.socket.emit('new-msg', {
          content: data.content, fromName: fromNick,
          type: data.type || 'text', burn: data.burn || false, msgId: data.msgId
        });
      }
    } catch (e) {}
  });

  socket.on('msg-read', (data) => {
    const p = userMap.get(user.partner);
    if (p && p.socket) p.socket.emit('msg-read', { msgId: data.msgId });
  });

  socket.on('disconnect', () => {
    cleanMatchTimer(sid);
    waitingUsers.delete(sid);
    if (user.username) {
      userOnlineSid.delete(user.username);
      usernameToSocket.delete(user.username);
    }
    userMap.delete(sid);
    broadcastOnlineCount();
    clearInterval(timer);
  });
});

startIdleCheck();
startKeepAliveCheck();
setInterval(broadcastOnlineCount, 30000);
loadPwd();
loadUsers();

// 保活
const SELF_URL = `http://127.0.0.1:${PORT}`;
setInterval(() => {
  http.get(SELF_URL).on('error',()=>{});
}, 180000);

// 启动
server.listen(PORT, '0.0.0.0', () => {
  console.log('=========================================');
  console.log('✅ 服务启动成功');
  console.log('✅ 在线人数修复');
  console.log('✅ 全局进场广播修复');
  console.log('✅ 匹配AI/真人修复');
  console.log('✅ 同账号互踢修复');
  console.log('✅ 1小时闲置下线修复');
  console.log('=========================================');
});
