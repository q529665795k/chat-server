const express = require('express');
const http = require('http');
const fs = require('fs');
const path = require('path');
const { Server } = require('socket.io');
const crypto = require('crypto');
const readline = require('readline');
const mysql = require('mysql2/promise');
const CryptoJS = require('crypto-js');
const axios = require('axios');

const ENCRYPTION_KEY = "beta644_key_2025";
const PORT = process.env.PORT || 6500;

function encrypt(text) {
  return CryptoJS.AES.encrypt(text, ENCRYPTION_KEY).toString();
}
function decrypt(ciphertext) {
  try {
    const bytes = CryptoJS.AES.decrypt(ciphertext, ENCRYPTION_KEY);
    return bytes.toString(CryptoJS.enc.Utf8);
  } catch (e) {
    return ciphertext;
  }
}

async function callAI(prompt) {
  try {
    const res = await axios.post("http://127.0.0.1:11434/api/chat", {
      model: "qwen2.5:0.5b",
      messages: [{ role: "user", content: prompt }],
      stream: false
    }, { timeout: 15000 });
    return res.data?.message?.content || "我在呢～";
  } catch (err) {
    return "AI服务异常";
  }
}

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
    console.log('✅ MySQL连接成功');

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
    console.error('❌ DB错误:', err.message);
  }
})();

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
const MATCH_TIMEOUT = 15000;
const REDIS_EXPIRE = 7200;

async function loadUsers() {
  try {
    const [rows] = await db.execute('SELECT username, nickname, password FROM users');
    loginMap.clear();
    rows.forEach(u => {
      loginMap.set(u.username, {
        nickname: u.nickname || u.username,
        password: u.password
      });
    });
  } catch (e) {}
}

function createMatchRoom(a, b) {
  const rid = `room_${Date.now()}_${Math.random()}`;
  roomMem.set(rid, { userA: a, userB: b, createTime: Date.now() });
  setTimeout(() => roomMem.delete(rid), REDIS_EXPIRE * 1000);
  return rid;
}

function saveOfflineMsg(uid, msg) {
  if (!offlineMsgMem.has(uid)) offlineMsgMem.set(uid, []);
  offlineMsgMem.get(uid).push(msg);
}

function pushOfflineMsg(socket, uid) {
  const list = offlineMsgMem.get(uid) || [];
  list.forEach(m => socket.emit('new-msg', m));
  offlineMsgMem.delete(uid);
}

function cleanMatchTimer(id) {
  if (userMatchTimer.has(id)) {
    clearTimeout(userMatchTimer.get(id));
    userMatchTimer.delete(id);
  }
}

function tryMatch() {
  const arr = Array.from(waitingUsers).map(id => userMap.get(id)).filter(u =>
    u && u.socket.connected && !u.partner && u.username
  );
  if (arr.length < 2) return;
  const u1 = arr[0], u2 = arr[1];
  if (u1.id === u2.id) return;

  cleanMatchTimer(u1.id);
  cleanMatchTimer(u2.id);
  waitingUsers.delete(u1.id);
  waitingUsers.delete(u2.id);

  u1.partner = u2.id;
  u2.partner = u1.id;
  u1.isMatched = true;
  u2.isMatched = true;

  const rid = createMatchRoom(u1.username, u2.username);
  u1.roomId = rid;
  u2.roomId = rid;

  const n1 = loginMap.get(u1.username)?.nickname || u1.username;
  const n2 = loginMap.get(u2.username)?.nickname || u2.username;

  u1.socket.emit('match-found', { partnerId: u2.id, partnerName: n2, roomId: rid });
  u2.socket.emit('match-found', { partnerId: u1.id, partnerName: n1, roomId: rid });
}

function assignAiRobot(id) {
  const u = userMap.get(id);
  if (!u || u.isMatched) return;
  u.partner = "ai_bot";
  u.isMatched = true;
  u.socket.emit('match-found', { partnerId: "ai_bot", partnerName: "AI陪伴者" });
}

function stopChat(id) {
  const me = userMap.get(id);
  if (!me || !me.partner) return;
  const p = userMap.get(me.partner);
  if (p && p.socket) {
    p.partner = null;
    p.isMatched = false;
    p.socket.emit('partner-leave');
  }
  me.partner = null;
  me.isMatched = false;
}

const app = express();
app.use(express.json());
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', 'https://im6.qzz.io');
  res.header('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  next();
});

app.get('/', (req, res) => res.send('Server Running'));

app.post('/register', async (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) return res.json({ code: 400 });
  try {
    const [exists] = await db.execute('SELECT username FROM users WHERE username=?', [username]);
    if (exists.length) return res.json({ code: 400, msg: '已存在' });
    await db.execute('INSERT INTO users (username, nickname, password) VALUES (?,?,?)',
      [username, username, password]);
    loginMap.set(username, { nickname: username, password });
    res.json({ code: 200, msg: 'ok' });
  } catch (e) {
    res.json({ code: 500 });
  }
});

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "https://www.im6.qzz.io" },
  pingTimeout: 60000,
  pingInterval: 25000
});

io.on('connection', (socket) => {
  const id = socket.id;
  const user = {
    id, socket, username: '', partner: null,
    isMatched: false, roomId: null
  };
  userMap.set(id, user);

  socket.on('checkLogin', async (data) => {
    const { userId } = data;
    if (!userId || !loginMap.has(userId)) return socket.emit('notLogin');
    user.username = userId;
    userSessionMap.set(userId, id);
    usernameToSocket.set(userId, socket);
    pushOfflineMsg(socket, userId);
    socket.emit('loginSuccess');
  });

  // ==============================
  // ✅ 昵称修改（用户名永远不动）
  // ==============================
  socket.on('change-nick', async (data) => {
    try {
      const { newNick } = data;
      const username = user.username;
      if (!username) return socket.emit('nick-result', { success: false, msg: '请登录' });
      if (!newNick || newNick.length < 2 || newNick.length > 20) {
        return socket.emit('nick-result', { success: false, msg: '长度2-20' });
      }
      await db.execute('UPDATE users SET nickname=? WHERE username=?', [newNick, username]);
      const info = loginMap.get(username);
      info.nickname = newNick;
      loginMap.set(username, info);
      socket.emit('nick-result', { success: true, newNick });
    } catch (e) {
      socket.emit('nick-result', { success: false, msg: '失败' });
    }
  });

  socket.on('match-chat', () => {
    if (!user.username) return;
    if (user.isMatched) stopChat(id);
    waitingUsers.add(id);
    const timer = setTimeout(() => assignAiRobot(id), MATCH_TIMEOUT);
    userMatchTimer.set(id, timer);
    tryMatch();
  });

  socket.on('send-msg', async (data) => {
    if (!user.username || !user.isMatched || !user.partner) return;
    const txt = decrypt(data.content);
    const to = userMap.get(user.partner);
    const toUser = to?.username || 'ai_bot';

    await db.execute('INSERT INTO messages (from_user, to_user, content, msg_id) VALUES (?,?,?,?)',
      [user.username, toUser, txt, data.msgId || '']);

    if (user.partner === 'ai_bot') {
      const reply = await callAI(txt);
      socket.emit('new-msg', {
        fromName: 'AI', content: encrypt(reply), msgId: data.msgId
      });
      return;
    }

    if (to && to.socket) {
      to.socket.emit('new-msg', {
        fromName: loginMap.get(user.username)?.nickname || user.username,
        content: data.content,
        msgId: data.msgId
      });
    }
  });

  socket.on('msg-read-confirm', async (data) => {
    const { msgId } = data;
    if (!msgId || !user.username) return;
    await db.execute('UPDATE messages SET is_read=1 WHERE msg_id=? AND to_user=?',
      [msgId, user.username]);
  });

  socket.on('disconnect', () => {
    if (user.username) {
      usernameToSocket.delete(user.username);
      userSessionMap.delete(user.username);
    }
    stopChat(id);
    userMap.delete(id);
    waitingUsers.delete(id);
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log('✅ 服务启动成功 端口：' + PORT);
  console.log('✅ 用户名固定 ✅ 昵称可改 ✅ 消息正常 ✅ 已读正常');
});
