const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mysql = require('mysql2/promise');
const cors = require('cors');
require('dotenv').config();

const app = express();
const server = http.createServer(app);
const PORT = process.env.PORT || 10000;

// ====================== 1. 核心跨域白名单 (在此添加新域名) ======================
const CORS_ORIGINS = [
  "https://im6.qzz.io",  // 主域名
  "http://im6.qzz.io",
  "https://im6.ct.ws",   // 备用域名
  "http://im6.ct.ws"
];

app.use(cors({
  origin: (origin, callback) => {
    // 允许白名单内的域名跨域，同时允许本地测试和无origin的请求(如移动端App)
    if (!origin || CORS_ORIGINS.includes(origin) || origin.includes('127.0.0.1')) {
      callback(null, true);
    } else {
      callback(new Error('CORS域名不匹配，请检查白名单'));
    }
  },
  credentials: true
}));
app.use(express.json({ limit: '20mb' }));

// ====================== 2. 数据库连接池 ======================
const pool = mysql.createPool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: 20
});

// ====================== 3. WebSocket 多端适配 & 互踢逻辑 ======================
const io = new Server(server, {
  cors: {
    origin: CORS_ORIGINS,
    credentials: true
  },
  allowEIO3: true,
  pingInterval: 10000, // 10秒发一次心跳包
  pingTimeout: 5000    // 5秒没回就断开，快速释放资源
});

let userMap = new Map();         // sid -> userObject
let userSessionMap = new Map();  // username -> sid (多账号互踢核心)

io.on('connection', (socket) => {
  const sid = socket.id;
  const user = { 
    id: sid, 
    username: '', 
    lastActive: Date.now() 
  };
  userMap.set(sid, user);

  // 用户上线 (联调点)
  socket.on('user-online', (data) => {
    const { username } = data;
    if (!username) return;

    // --- 【多账号互踢逻辑】 ---
    if (userSessionMap.has(username)) {
      const oldSid = userSessionMap.get(username);
      if (oldSid !== sid) {
        io.to(oldSid).emit('force_logout', { msg: '账号在其他设备登录，你已被迫下线' });
        io.sockets.sockets.get(oldSid)?.disconnect();
      }
    }

    user.username = username;
    userSessionMap.set(username, sid);

    // 广播实时在线人数
    io.emit('online_count', { count: userSessionMap.size });
    
    // 全员广播进场
    io.emit('broadcast_msg', {
      user: '系统',
      text: `${data.nickname || username} 进入了基地`,
      type: 'system'
    });
  });

  // --- 【心跳保活检测】 ---
  socket.on('HEARTBEAT', () => {
    user.lastActive = Date.now();
    socket.emit('HEARTBEAT_ACK');
  });

  // --- 【全员广播发送】 ---
  socket.on('send_global_broadcast', (data) => {
    if (!user.username) return;
    io.emit('broadcast_msg', {
      user: data.nickname || user.username,
      text: data.text,
      time: new Date().toLocaleTimeString(),
      type: 'user'
    });
  });

  socket.on('disconnect', () => {
    if (user.username) {
      // 只有当前连接确实是这个用户的活跃连接时才删除
      if (userSessionMap.get(user.username) === sid) {
        userSessionMap.delete(user.username);
      }
    }
    userMap.delete(sid);
    io.emit('online_count', { count: userSessionMap.size });
  });
});

// ====================== 4. 闲置超时检测 (1小时自动下线) ======================
setInterval(() => {
  const now = Date.now();
  userMap.forEach((u, sid) => {
    // 3600000ms = 1小时
    if (now - u.lastActive > 3600000) {
      io.to(sid).emit('force_logout', { msg: '由于您长时间不在线，系统已自动断开' });
      io.sockets.sockets.get(sid)?.disconnect();
    }
  });
}, 60000); // 每分钟巡检一次

// ====================== 5. 业务路由 (登录/注册) ======================
app.post('/api/login', async (req, res) => {
  const { username, password } = req.body;
  try {
    const [rows] = await pool.query("SELECT * FROM users WHERE username=? AND password=?", [username, password]);
    if (rows.length > 0) {
      res.json({ code: 200, data: rows[0], msg: '登录成功' });
    } else {
      res.json({ code: 400, msg: '账号或密码错误' });
    }
  } catch (e) { res.status(500).json({ code: 500, msg: '服务器异常' }); }
});

// ... 注册及其他接口保持不变 ...

server.listen(PORT, '0.0.0.0', () => {
  console.log(`=========================================`);
  console.log(`✅ 后端双域名版部署成功！`);
  console.log(`✅ 支持域名: ${CORS_ORIGINS.join(' | ')}`);
  console.log(`=========================================`);
});
