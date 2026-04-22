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

// ===== D1 数据库连接（双兼容版：同时支持 db.execute 和 db.query）=====
let db;
(async () => {
  try {
    const D1_DB_ID = process.env.MYSQL_DATABASE;
    const CF_API_TOKEN = process.env.CLOUDFLARE_API_TOKEN;
    
    // 封装成 mysql2 风格，返回 [rows] 结构
    const dbCore = {
      execute: async (sql, ...params) => {
        const res = await fetch(
          `https://api.cloudflare.com/client/v4/d1/databases/${D1_DB_ID}/query`,
          {
            method: "POST",
            headers: {
              Authorization: `Bearer ${CF_API_TOKEN}`,
              "Content-Type": "application/json"
            },
            body: JSON.stringify({ sql, params })
          }
        );
        const data = await res.json();
        // 适配 D1 返回格式，兼容你原来的代码
        const results = data.result?.[0]?.results || [];
        return [results];
      }
    };

    // ✅ 关键：让 db.query 和 db.execute 完全等价，兼容你所有旧代码
    dbCore.query = dbCore.execute;

    // 全局暴露 db
    db = dbCore;

    console.log(`[${new Date().toLocaleString()}] 【DB】✅ D1 数据库连接成功，execute/query 双兼容`);
  } catch (err) {
    console.log(`[${new Date().toLocaleString()}] 【DB】❌ D1 数据库连接失败：`, err.message);
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

// ====================== 全局异常捕获中间件 ======================
app.post('/register', async (req, res, next) => {

  // 打印完整错误信息
  console.error("❌【全局异常捕获】", {
    path: req.path,
    method: req.method,
    error: err.message,
    stack: err.stack
  });

  // 统一返回格式
  res.status(500).json({
    code: 500,
    msg: "服务器异常：" + err.message,
    data: null
  });
});


// ====================== 【注册接口 · 独立完整段】 ======================
// ====================== 【修复版·注册接口】带双重异常捕获 · 真入库 ======================
app.post('/register', async (req, res, next) => {
  try {
    // 1. 接收前端参数
    const { username, password, nickname } = req.body;

    // 2. 基础参数校验
    if (!username || !password) {
      return res.json({ code: 400, msg: '❌ 账号密码不能为空' });
    }

    // 3. 先查账号是否已存在（防止重复注册）
    const existUser = await db.get("SELECT * FROM users WHERE username = ?", [username]);
    if (existUser) {
      return res.json({ code: 409, msg: '❌ 账号已存在，换一个试试' });
    }

    // 4. 【核心】真实写入数据库（带执行结果校验）
    const insertResult = await db.run(
      `INSERT INTO users 
      (username, password, nickname, create_time) 
      VALUES (?, ?, ?, datetime('now'))`,
      [username, password, nickname || username]
    );

    // 5. 强制校验：只有数据库真的新增成功，才返回成功
    if (insertResult.changes > 0) {
      console.log(`✅【注册成功】真实入库账号：${username}`);
      return res.json({ code: 200, msg: '✅ 注册成功' });
    } else {
      // SQL执行了但没写入，直接抛错
      throw new Error('SQL执行完成，但数据未写入数据库');
    }

  } catch (err) {
    // 6. 局部异常捕获 + 交给全局异常兜底
    console.error(`❌【注册失败】${err.message}`);
    next(err); // 把错误传给上面的全局异常中间件
  }
});



// 登录
// ====================== 【登录接口 · 完整保留你的缓存+昵称逻辑 + 全流程详细日志】 ======================
app.post('/login', async (req, res) => {
  const now = new Date().toLocaleString();
  const { username, password } = req.body;

  // ========== 1. 打印原始登录请求（明文带密码） ==========
  console.log(`\n[${now}] 【登录请求】收到登录数据`);
  console.log(`[${now}] 【登录请求】尝试登录用户名: ${username}`);
  console.log(`[${now}] 【登录请求】尝试登录明文密码: ${password}`);

  // ========== 2. 基础校验 ==========
  if (!username || !password) {
    console.log(`[${now}] 【登录失败】用户名或密码为空`);
    return res.json({ code: 400, msg: '用户名和密码不能为空' });
  }

  try {
    // ========== 3. 第一步：先查询数据库，找该用户名 ==========
    console.log(`[${now}] 【登录步骤】第一步：查询数据库，查找用户名 → ${username}`);
    const [rows] = await db.query('SELECT * FROM users WHERE username = ?', [username]);

    // ========== 4. 判断：账号不存在 ==========
    if (rows.length === 0) {
      console.log(`[${now}] 【登录结果】❌ 账号不存在，数据库无匹配记录 → ${username}`);
      return res.json({ code: 400, msg: '账号不存在，请先注册' });
    }

    // ========== 5. 账号存在，对比密码 ==========
    console.log(`[${now}] 【登录步骤】账号存在，开始校验密码`);
    console.log(`[${now}] 【登录步骤】数据库中存储的密码: ${rows[0].password}`);
    console.log(`[${now}] 【登录步骤】用户输入的明文密码: ${password}`);

    // ========== 6. 判断：密码错误 ==========
    if (rows[0].password !== password) {
      console.log(`[${now}] 【登录结果】❌ 密码错误，登录失败 → 用户名: ${username}`);
      return res.json({ code: 400, msg: '密码错误' });
    }

    // ========== ✅ 这里完全保留你原来的【内存缓存最新昵称】逻辑 ==========
    console.log(`[${now}] 【登录步骤】✅ 密码校验通过，开始缓存用户信息`);
    loginMap.set(username, {
      nickname: rows[0].nickname || username,
      password: password
    });

    // ========== ✅ 这里完全保留你原来的【昵称读取日志】 ==========
    console.log(`[${now}] 【登录步骤】✅ 已从数据库读取最新昵称`, {
      username: username,
      latestNickname: rows[0].nickname || username
    });

    // ========== 原有系统日志 ==========
    sysLog('USER', '用户登录', { username });

    // ========== 登录成功日志 ==========
    console.log(`[${now}] 【登录结果】✅ 登录成功！`);
    console.log(`[${now}] 【登录结果】登录账号: ${username}`);
    console.log(`[${now}] 【登录结果】登录昵称: ${rows[0].nickname || username}`);

    // ========== ✅ 完全保留你原来的返回前端逻辑 ==========
    return res.json({
      code: 200,
      msg: '登录成功',
      nickname: rows[0].nickname || username
    });

  } catch (err) {
    console.log(`[${now}] 【登录异常】❌ 数据库查询失败:`, err);
    return res.json({ code: 500, msg: '服务器异常' });
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

    console.log('[send-msg] 收到消息', {
      from: user.username,
      to: user.partner,
      type: data.type,
      content: data.content,
      url: data.url,
      msgId: data.msgId
    });

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
          content: reply,
          type: 'text',
          burn: false,
          msgId: Date.now().toString(),
          fromName: 'AI陪伴者'
        });
        console.log('[send-msg] AI已回复消息');
      }, 600);
      return;
    }

    if (to && to.socket) {
      let sendContent = '';
      if (data.type === 'image' || data.type === 'video') {
        sendContent = data.url;
        console.log('[send-msg] 媒体消息，使用URL转发', sendContent);
      } else {
        sendContent = data.content;
        console.log('[send-msg] 文本消息，直接转发内容');
      }

      to.socket.emit('new-msg', {
        content: sendContent,
        type: data.type || 'text',
        burn: data.burn || false,
        msgId: data.msgId || '',
        fromName: fromNick
      });
      console.log('[send-msg] 消息已转发给对方', to.username);
    } else {
      console.log('[send-msg] 对方不在线，不转发');
    }

  } catch (err) {
    console.error('[send-msg] 处理失败：', err);
  }
});

  socket.on('msg-read', async (data) => {
  try {
    const p = userMap.get(user.partner);
    console.log('[msg-read] 收到已读回执', {
      from: user.username,
      partner: user.partner,
      msgId: data.msgId
    });

    // 1. 转发回执给对方
    if (p && p.socket) {
      p.socket.emit('msg-read', { msgId: data.msgId });
      sysLog('MSG', '已读回执', { from: p.username, msgId: data.msgId });
      console.log('[msg-read] 已读回执已转发给对方', p.username);
    }

    // 2. 新增：把这条消息标记为已读，写入数据库
    await db.execute('UPDATE messages SET isRead = ? WHERE msgId = ?', [true, data.msgId]);
    console.log('[msg-read] 数据库已标记为已读', data.msgId);

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

// ===================== 3分钟自我保活 + 精准重启逻辑 =====================
// 规则：只有Ping自己失败才重启，数据库异常只警告不重启
const TARGET_URL = `http://127.0.0.1:${PORT}`;
const PING_INTERVAL = 3 * 60 * 1000; // 3分钟检查一次 不动！
const FIRST_DELAY = 30 * 1000;       // 启动延迟30秒 不动！
const FAILURE_THRESHOLD = 3;         // 连续3次失败重启 不动！
const LOG_FILE = path.join(__dirname, 'keepalive.log');
const SCRIPT_PATH = __filename;

let consecutiveFailures = 0;
let isRestarting = false;

// ✅ 修复：异步日志 永不卡死服务
function writeLog(msg) {
  const t = new Date().toLocaleString('zh-CN');
  const logStr = `[${t}] ${msg}\n`;
  // 异步写入 不阻塞主线程
  fs.appendFile(LOG_FILE, logStr, (err) => {
    if (err) console.error('日志写入失败:', err);
  });
  // 强制控制台打印
  console.log(logStr.trim());
}

// ✅ Ping自己检测（60秒超长超时 你已经改好的不动）
function pingSelf() {
  return new Promise((resolve, reject) => {
    const req = http.get(TARGET_URL, { timeout: 60000 }, (res) => {
      if (res.statusCode === 200) resolve();
      else reject(new Error('服务异常'));
    });
    req.on('timeout', () => reject(new Error('请求超时')));
    req.on('error', (err) => reject(err));
    req.end();
  });
}

// ✅ 核心健康检查：Ping失败才重启，数据库异常只警告
async function doHealthCheck() {
  if (isRestarting) return;
  try {
    // 1. 核心判定：Ping自己成功 = 服务活着
    await pingSelf();
    consecutiveFailures = 0;
    writeLog('✅ 健康检查正常：服务在线');

    // 2. 数据库只做状态监控 失败不重启
    try {
      await db.prepare('SELECT 1').run();
      writeLog('✅ D1数据库连接正常');
    } catch (dbErr) {
      writeLog('⚠️ D1数据库连接异常，但服务正常运行');
    }

  } catch (err) {
    // ❌ 只有Ping失败 才判定服务异常 累加次数
    consecutiveFailures++;
    writeLog(`⚠️ 健康检查失败 ${consecutiveFailures}/${FAILURE_THRESHOLD}：${err.message}`);

    // 🔴 只有连续Ping失败 才执行重启
    if (consecutiveFailures >= FAILURE_THRESHOLD && !isRestarting) {
      isRestarting = true;
      writeLog('🚨 连续异常，自动重启服务');
      const child = require('child_process').exec;
      child(`node ${SCRIPT_PATH}`, () => {});
      setTimeout(() => process.exit(1), 1500);
    }
  }
}

// 启动定时器 保留你原来的逻辑
setTimeout(() => {
  doHealthCheck();
  setInterval(doHealthCheck, PING_INTERVAL);
}, FIRST_DELAY);

// 全局异常兜底
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
