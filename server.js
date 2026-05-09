const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mysql = require('mysql2/promise');
const cors = require('cors');
const axios = require('axios'); // 新增用于自Ping
require('dotenv').config();

const app = express();
const server = http.createServer(app);
const PORT = process.env.PORT || 10000;

// ====================== 1. 核心跨域白名单 ======================
const CORS_ORIGINS = [
  "https://im6.qzz.io", "http://im6.qzz.io",
  "https://im6.ct.ws", "http://im6.ct.ws"
];

app.use(cors({
  origin: (origin, callback) => {
    if (!origin || CORS_ORIGINS.includes(origin) || origin.includes('127.0.0.1')) {
      callback(null, true);
    } else {
      callback(new Error('CORS域名不匹配'));
    }
  },
  credentials: true
}));
app.use(express.json({ limit: '20mb' }));

// ====================== 2. 数据库连接池 (增强配置) ======================
const pool = mysql.createPool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: 20,
  enableKeepAlive: true,
  keepAliveInitialDelay: 10000
});

// ====================== 3. 【核心】双重保活逻辑 ======================

// (1) 15分钟数据库 Ping 保活：防止DB连接超时断开
setInterval(async () => {
  try {
    await pool.query('SELECT 1');
    console.log('💓 DB保活成功 - ' + new Date().toLocaleString());
  } catch (err) {
    console.error('❌ DB连接异常:', err.message);
  }
}, 900000); // 15分钟

// (2) 3分钟本机自Ping保活：防止 Render/Railway 等平台因无流量而让服务休眠
const SELF_URL = `http://localhost:${PORT}/api/ping`;
app.get('/api/ping', (req, res) => res.send('pong'));

setInterval(async () => {
  try {
    // 优先尝试使用你当前正在运行的外部域名，备用本地端口
    const pingUrl = CORS_ORIGINS[0] + "/api/ping";
    await axios.get(pingUrl);
    console.log('🚀 本机自Ping成功 - 确保服务不休眠');
  } catch (err) {
    // 如果外部Ping不通，尝试本地环回
    try { await axios.get(`http://127.0.0.1:${PORT}/api/ping`); } catch(e){}
  }
}, 180000); // 3分钟

// ====================== 4. WebSocket & 互踢逻辑 ======================
const io = new Server(server, {
  cors: { origin: CORS_ORIGINS, credentials: true },
  pingInterval: 10000,
  pingTimeout: 5000
});

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
      if (oldSid !== sid) {
        io.to(oldSid).emit('force_logout', { msg: '账号在其他设备登录，你已被迫下线' });
        io.sockets.sockets.get(oldSid)?.disconnect();
      }
    }

    user.username = username;
    userSessionMap.set(username, sid);
    io.emit('online_count', { count: userSessionMap.size });
    io.emit('broadcast_msg', { user: '系统', text: `${data.nickname || username} 进入了基地`, type: 'system' });
  });

  socket.on('HEARTBEAT', () => {
    user.lastActive = Date.now();
    socket.emit('HEARTBEAT_ACK');
  });

  socket.on('disconnect', () => {
    if (user.username && userSessionMap.get(user.username) === sid) {
      userSessionMap.delete(user.username);
    }
    userMap.delete(sid);
    io.emit('online_count', { count: userSessionMap.size });
  });
});

// ====================== 5. 1小时闲置清理 ======================
setInterval(() => {
  const now = Date.now();
  userMap.forEach((u, sid) => {
    if (now - u.lastActive > 3600000) {
      io.to(sid).emit('force_logout', { msg: '由于您长时间未操作，系统已自动断开' });
      io.sockets.sockets.get(sid)?.disconnect();
    }
  });
}, 60000);

// ====================== 6. 业务接口 ======================
app.post('/api/login', async (req, res) => {
  const { username, password } = req.body;
  try {
    const [rows] = await pool.query("SELECT * FROM users WHERE username=? AND password=?", [username, password]);
    if (rows.length > 0) res.json({ code: 200, data: rows[0], msg: '登录成功' });
    else res.json({ code: 400, msg: '账号或密码错误' });
  } catch (e) { res.status(500).json({ code: 500, msg: '异常' }); }
});

// 注册及其他接口...

server.listen(PORT, '0.0.0.0', () => {
  console.log(`✅ 后端双域名版已启动，保活逻辑已全面就绪！`);
});
