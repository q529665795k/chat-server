const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mysql = require('mysql2/promise');
const cors = require('cors');
const axios = require('axios');
require('dotenv').config();

const app = express();
const server = http.createServer(app);
const PORT = process.env.PORT || 10000;

// 日志系统
const log = {
  info: (msg) => console.log(`[\x1b[32mINFO\x1b[0m] ${new Date().toLocaleString()} - ${msg}`),
  warn: (msg) => console.log(`[\x1b[33mWARN\x1b[0m] ${new Date().toLocaleString()} - ${msg}`),
  err: (msg) => console.log(`[\x1b[31mERROR\x1b[0m] ${new Date().toLocaleString()} - ${msg}`),
  sys: (msg) => console.log(`[\x1b[36mSYSTEM\x1b[0m] ${new Date().toLocaleString()} - ${msg}`)
};

// ====================== 1. 跨域白名单 ======================
const CORS_ORIGINS = ["https://im6.qzz.io", "http://im6.qzz.io", "https://im6.ct.ws", "http://im6.ct.ws"];
app.use(cors({
  origin: (origin, callback) => {
    if (!origin || CORS_ORIGINS.includes(origin) || origin.includes('127.0.0.1')) callback(null, true);
    else { log.warn(`非法跨域访问: ${origin}`); callback(null, false); }
  },
  credentials: true
}));
app.use(express.json({ limit: '20mb' }));

// ====================== 2. 数据库池 (防注入核心) ======================
const pool = mysql.createPool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: 20,
  enableKeepAlive: true
});

// 15分钟数据库保活
setInterval(async () => {
  try { await pool.query('SELECT 1'); log.sys("💓 数据库连接保活成功"); } 
  catch (err) { log.err(`数据库连接异常: ${err.message}`); }
}, 900000);

// 3分钟本机自Ping
app.get('/api/ping', (req, res) => res.send('pong'));
setInterval(async () => {
  try { await axios.get(`http://127.0.0.1:${PORT}/api/ping`); log.sys("🚀 本机活跃Ping成功"); } 
  catch (e) { log.warn("本机Ping失败，检查网络"); }
}, 180000);

// ====================== 3. 业务接口 (防注入逻辑) ======================

// 登录接口：堵死万能密码，严谨逻辑判断
app.post('/api/login', async (req, res) => {
  const { username, password } = req.body;
  log.info(`登录尝试: ${username}`);
  
  try {
    // 使用参数化查询 ? ，用户输入的任何内容都会被视为普通文本，无法逃逸
    const [rows] = await pool.query("SELECT * FROM users WHERE username = ?", [username]);
    
    if (rows.length === 0) {
      log.warn(`登录失败: 账号 [${username}] 不存在`);
      return res.json({ code: 400, msg: "账号不存在，请先注册" });
    }

    const user = rows[0];
    // 严格对比文本密码
    if (user.password === password) {
      log.info(`登录成功: ${username}`);
      res.json({ code: 200, data: { username: user.username, nickname: user.nickname }, msg: "欢迎回来" });
    } else {
      log.warn(`登录失败: 用户 [${username}] 密码错误`);
      res.json({ code: 400, msg: "密码错误，请重试" });
    }
  } catch (e) {
    log.err(`登录异常: ${e.message}`);
    res.json({ code: 500, msg: "服务器忙，请稍后再试" });
  }
});

// 注册接口：防注入 + 查重
app.post('/api/register', async (req, res) => {
  const { username, password } = req.body;
  try {
    const [exist] = await pool.query("SELECT id FROM users WHERE username = ?", [username]);
    if (exist.length > 0) return res.json({ code: 400, msg: "该代号已被占用" });

    await pool.query("INSERT INTO users (username, password, nickname) VALUES (?, ?, ?)", [username, password, username]);
    log.info(`新用户注册成功: ${username}`);
    res.json({ code: 200, msg: "注册成功，快去上线吧" });
  } catch (e) {
    log.err(`注册报错: ${e.message}`);
    res.json({ code: 500, msg: "注册失败" });
  }
});

// 修改昵称接口
app.post('/update-nickname', async (req, res) => {
  const { username, newNickname } = req.body;
  try {
    await pool.query("UPDATE users SET nickname = ? WHERE username = ?", [newNickname, username]);
    log.info(`用户 [${username}] 修改昵称为: ${newNickname}`);
    res.json({ code: 200, msg: "修改成功" });
  } catch (e) { log.err(`改名报错: ${e.message}`); res.json({ code: 500 }); }
});

// ====================== 4. WebSocket & 互踢逻辑 (保持不变) ======================
const io = new Server(server, { cors: { origin: CORS_ORIGINS, credentials: true } });
let userMap = new Map();
let userSessionMap = new Map();

io.on('connection', (socket) => {
  const sid = socket.id;
  const user = { id: sid, username: '', lastActive: Date.now() };
  userMap.set(sid, user);

  socket.on('user-online', (data) => {
    const { username } = data;
    if (!username) return;

    // 多账号互踢
    if (userSessionMap.has(username)) {
      const oldSid = userSessionMap.get(username);
      log.warn(`踢出冲突账号: ${username}`);
      io.to(oldSid).emit('force_logout', { msg: '账号在别处登录，你已被迫下线' });
      io.sockets.sockets.get(oldSid)?.disconnect();
    }

    user.username = username;
    userSessionMap.set(username, sid);
    io.emit('online_count', { count: userSessionMap.size });
    log.info(`Socket关联用户成功: ${username}`);
  });

  socket.on('HEARTBEAT', () => { user.lastActive = Date.now(); socket.emit('HEARTBEAT_ACK'); });

  socket.on('disconnect', () => {
    if (user.username) userSessionMap.delete(user.username);
    userMap.delete(sid);
    io.emit('online_count', { count: userSessionMap.size });
  });
});

// 1小时闲置清理
setInterval(() => {
  const now = Date.now();
  userMap.forEach((u, sid) => {
    if (now - u.lastActive > 3600000) {
      log.warn(`用户 [${u.username}] 闲置超时被踢出`);
      io.to(sid).emit('force_logout', { msg: '由于长时间未操作，已自动断开' });
      io.sockets.sockets.get(sid)?.disconnect();
    }
  });
}, 60000);

server.listen(PORT, '0.0.0.0', () => {
  log.sys(`=========================================`);
  log.sys(`摸鱼基地 V2.0 启动成功！`);
  log.sys(`SQL注入防御已开启 | 双重保活已就绪`);
  log.sys(`=========================================`);
});
