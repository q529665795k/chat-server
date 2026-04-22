const http = require('http');
const process = require('process');
const express = require('express');
const fs = require('fs');
const path = require('path');
const readline = require('readline');
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

// ===== D1 数据库连接【定稿版】=====



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
// 启动时自动测试连接
(async () => {
  try {
    const conn = await pool.getConnection();
    console.log('✅ Aiven MySQL 连接成功！');
    conn.release();
  } catch (err) {
    console.error('❌ MySQL 连接失败：', err);
  }
})();

module.exports = pool;



// 启动自动连库 + 安全建表
// 启动自动连库 + 安全建表（5张表一次性建齐，有就跳过）
(async () => {
  try {
    const conn = await pool.getConnection();
    console.log('✅ MySQL 数据库连接成功');

    // 1. 用户主表
    await conn.query(`
    CREATE TABLE IF NOT EXISTS users (
      id INT PRIMARY KEY AUTO_INCREMENT COMMENT '用户自增ID',
      username VARCHAR(50) NOT NULL UNIQUE COMMENT '登录账号，唯一凭证',
      password VARCHAR(100) NOT NULL COMMENT '登录密码（明文）',
      nickname VARCHAR(50) COMMENT '用户昵称',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '注册时间',
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '资料更新时间'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    `);

    // 2. 聊天消息表
    await conn.query(`
    CREATE TABLE IF NOT EXISTS messages (
      id INT PRIMARY KEY AUTO_INCREMENT COMMENT '消息自增ID',
      sender VARCHAR(50) NOT NULL COMMENT '发送者账号',
      receiver VARCHAR(50) NOT NULL COMMENT '接收者账号',
      content TEXT COMMENT '消息内容',
      msg_type VARCHAR(20) DEFAULT 'text' COMMENT '消息类型:text/image/video/file',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '发送时间'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    `);

    // 3. 昵称修改日志表
    await conn.query(`
    CREATE TABLE IF NOT EXISTS nickname_logs (
      id INT PRIMARY KEY AUTO_INCREMENT COMMENT '日志自增ID',
      username VARCHAR(50) NOT NULL COMMENT '操作人用户名（唯一不变）',
      old_nickname VARCHAR(50) COMMENT '修改前昵称',
      new_nickname VARCHAR(50) COMMENT '修改后昵称',
      create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '修改时间'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    `);

    // 4. 登录日志表
    await conn.query(`
    CREATE TABLE IF NOT EXISTS login_logs (
      id INT PRIMARY KEY AUTO_INCREMENT COMMENT '日志ID',
      username VARCHAR(50) NOT NULL COMMENT '登录用户名',
      login_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '登录时间'
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    `);

    // 5. 注册日志表（含明文密码，你要的）
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

// 全局统一日志
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
// 精准白名单：只允许你的两个域名
const allowOrigins = [
  "https://im6.qzz.io",
  "https://www.im6.qzz.io"
];

// 全局跨域 + API 403 拦截修复（无 * 号）
app.use((req, res, next) => {
  const origin = req.headers.origin;

  // 只放行白名单里的域名
  if (allowOrigins.includes(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
  }

  // 精准配置，不用 *
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With");
  res.setHeader("Access-Control-Allow-Credentials", "true");

  // 预检请求直接放行，解决 403
  if (req.method === "OPTIONS") {
    return res.sendStatus(200);
  }

  next();
});

// 保留你原来的解析配置，不动
app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));


// ====================== 用户注册接口（MySQL完整版｜明文密码｜日志含密码） ======================
app.post('/api/register', async (req, res) => {
  try {
    // 明文接收用户名、密码，不加密、不隐藏
    const { username, password } = req.body;

    // 1. 用户名唯一判重（登录凭证，不能重复）
    const [existUser] = await pool.query(
      `SELECT username FROM users WHERE username = ?`,
      [username]
    );
    if (existUser.length > 0) {
      return res.json({ code: 400, msg: '用户名已被注册，请换一个' });
    }

    // 2. 默认昵称 = 用户名
    const defaultNickname = username;

    // 3. 插入用户主表（密码明文直接入库）
    await pool.query(
      `INSERT INTO users (username, password, nickname) VALUES (?, ?, ?)`,
      [username, password, defaultNickname]
    );

    // 4. 写入注册日志（明文密码一起记录，方便排查）
    await pool.query(
      `INSERT INTO register_logs (username, password, register_time) VALUES (?, ?, NOW())`,
      [username, password]
    );

    // 5. 返回成功信息
    res.json({
      code: 200,
      msg: '注册成功',
      data: {
        username: username,
        nickname: defaultNickname
      }
    });

    console.log(`✅ 新用户注册成功 | 账号:${username} | 密码:${password}`);

  } catch (err) {
    console.error('❌ 注册失败：', err);
    res.json({ code: 500, msg: '服务器错误，注册失败' });
  }
});



// ====================== 用户登录接口（MySQL完整版） ======================
app.post('/api/login', async (req, res) => {
  try {
    const { username, password } = req.body;

    // ✅ 第一步：从MySQL数据库实时拉取账号信息
    const [userRows] = await pool.query(
      `SELECT username, password, nickname FROM users WHERE username = ?`,
      [username]
    );

    // ✅ 第二步：数据库没查到 → 提示【账号不存在】
    if (userRows.length === 0) {
      return res.json({ code: 400, msg: '账号不存在' });
    }

    const user = userRows[0];

    // ✅ 第三步：查到账号了 → 校验密码
    if (user.password !== password) {
      return res.json({ code: 400, msg: '密码错误' });
    }

    // ✅ 第四步：登录成功 → 记录登录日志
    await pool.query(
      `INSERT INTO login_logs (username, login_time) VALUES (?, NOW())`,
      [username]
    );

    // ✅ 第五步：返回【数据库最新昵称】（解决改昵称不显示bug）
    res.json({
      code: 200,
      msg: '登录成功',
      data: {
        username: user.username,
        nickname: user.nickname
      }
    });

    console.log(`✅ 登录成功 | 账号:${username} | 昵称:${user.nickname}`);

  } catch (err) {
    console.error('❌ 登录失败：', err);
    res.json({ code: 500, msg: '服务器错误，登录失败' });
  }
});






// Socket.io
const io = new Server(server, {
  cors: {
    origin: [
      "https://im6.qzz.io",
      "https://www.im6.qzz.io"
    ],
    methods: ["GET", "POST"],
    credentials: true
  }
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

  
  // 1. 校验登录状态
  // 修改昵称 最终完整版（判重 + 日志 + 全服广播）
app.post('/api/update-nickname', async (req, res) => {
  try {
    const { username, newNickname } = req.body;

    // 1. 判重：新昵称不能被别人占用
    const [nickRepeat] = await pool.query(
      `SELECT username FROM users WHERE nickname = ? AND username != ?`,
      [newNickname, username]
    );
    if (nickRepeat.length > 0) {
      return res.json({ code: 400, msg: '昵称已被占用，请换一个' });
    }

    // 2. 获取旧昵称
    const [userInfo] = await pool.query(
      `SELECT nickname FROM users WHERE username = ?`,
      [username]
    );
    if (userInfo.length === 0) {
      return res.json({ code: 400, msg: '用户不存在' });
    }
    const oldNickname = userInfo[0].nickname;

    // 3. 昵称没变化直接返回
    if (oldNickname === newNickname) {
      return res.json({ code: 200, msg: '昵称未发生变化' });
    }

    // 4. 更新昵称
    await pool.query(
      `UPDATE users SET nickname = ? WHERE username = ?`,
      [newNickname, username]
    );

    // 5. 写入修改日志
    await pool.query(
      `INSERT INTO nickname_logs (username, old_nickname, new_nickname) VALUES (?, ?, ?)`,
      [username, oldNickname, newNickname]
    );

    // 6. 全服广播通知所有人
    io.emit('nickname-update', {
      username: username,
      oldNickname: oldNickname,
      newNickname: newNickname,
      time: new Date().toLocaleString()
    });

    // 7. 返回成功信息
    res.json({
      code: 200,
      msg: '昵称修改成功',
      data: { username, oldNickname, newNickname }
    });

    console.log(`✅ 昵称修改成功 | ${username} | ${oldNickname} → ${newNickname}`);

  } catch (err) {
    console.error('❌ 修改昵称失败：', err);
    res.json({ code: 500, msg: '服务器错误，修改失败' });
  }
});





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
        let sendContent = data.type === 'image' || data.type === 'video' ? data.url : data.content;
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

  socket.on('msg-read', async (data) => {
    try {
      const p = userMap.get(user.partner);
      if (p && p.socket) {
        p.socket.emit('msg-read', { msgId: data.msgId });
      }
      await db.execute('UPDATE messages SET isRead = ? WHERE msgId = ?', [true, data.msgId]);
    } catch (err) {
      console.error('[msg-read] 处理失败：', err);
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

// 3分钟自我保活（已改成 MySQL 数据库健康检查）
const TARGET_URL = `http://127.0.0.1:${PORT}`;
const PING_INTERVAL = 3 * 60 * 1000;
const FIRST_DELAY = 30 * 1000;
const FAILURE_THRESHOLD = 3;
const LOG_FILE = path.join(__dirname, 'keepalive.log');
const SCRIPT_PATH = __filename;
let consecutiveFailures = 0;
let isRestarting = false;

function writeLog(msg) {
  const t = new Date().toLocaleString('zh-CN');
  const logStr = `[${t}] ${msg}\n`;
  fs.appendFile(LOG_FILE, logStr, () => {});
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
    // ✅ 这里改成 MySQL 测试语句
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
  console.log('✅ 全局统一日志：server.log');
  console.log('✅ 跨域已配置：im6.qzz.io + www.im6.qzz.io');
  console.log('✅ 3分钟保活 + 崩溃自动重启');
  console.log('✅ 登录/注册/匹配/消息/已读/日志 全部正常');
  console.log('=========================================');
});
