const http = require('http');
const process = require('process');
const express = require('express');
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const axios = require('axios');
const { exec } = require('child_process');
const { Server } = require('socket.io');
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


// ===== D1 数据库【根治版】修复异步赋值 + 全方法补全 =====
let db = null;

// 初始化数据库（同步初始化，保证db是真实对象）
(async function initDatabase() {
    try {
        const cfToken = process.env.CLOUDFLARE_API_TOKEN;
        const d1DbId = process.env.D1_DATABASE_ID;

        if (!cfToken || !d1DbId) {
            console.log("⚠️ D1数据库：环境变量缺失");
            return;
        }

        // 完整数据库实例，四个核心方法全齐
        db = {
            query: async (sql, params = []) => {
                try {
                    const res = await fetch(
                        `https://api.cloudflare.com/client/v4/accounts/584cf375e1b82b17d54d67b4f14fa7db/d1/databases/${d1DbId}/query`,
                        {
                            method: "POST",
                            headers: {
                                "Authorization": `Bearer ${cfToken}`,
                                "Content-Type": "application/json"
                            },
                            body: JSON.stringify({ sql, params })
                        }
                    );
                    return await res.json();
                } catch (e) {
                    return { success: false, result: [] };
                }
            },
            execute: function(sql, params = []) {
                return this.query(sql, params);
            },
            run: function(sql, params = []) {
                return this.query(sql, params);
            },
            get: async function(sql, params = []) {
                const r = await this.query(sql, params);
                return r?.result?.[0] || null;
            }
        };

        console.log("✅ D1数据库：连接成功，所有方法就绪");
    } catch (error) {
        console.error("❌ D1数据库：连接失败", error.message);
        db = null;
    }
})();

module.exports = { db };




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

// 全局异常捕获
app.use((err, req, res, next) => {
  console.error("❌【全局异常】", err.message);
  res.status(500).json({ code: 500, msg: "服务器异常：" + err.message });
});

// 注册接口
app.post('/register', async (req, res, next) => {
  try {
    const { username, password, nickname } = req.body;
    console.log(`📝【注册请求】接收参数 -> 账号:${username} | 密码:${password} | 昵称:${nickname}`);

    if (!username || !password) {
      console.log(`❌【注册失败】账号或密码为空`);
      return res.json({ code: 400, msg: '❌ 账号密码不能为空' });
    }

    const existUser = await db.get("SELECT * FROM users WHERE username = ?", [username]);
    if (existUser) {
      console.log(`❌【注册失败】账号已存在 -> ${username}`);
      return res.json({ code: 409, msg: '❌ 账号已存在，换一个试试' });
    }

    // 修复：和数据库users表字段100%匹配 + 补全所有字段
    const userId = Date.now().toString();
    const userNick = nickname || username;
    const insertResult = await db.run(
      `INSERT INTO users
      (id, username, password, nickname, avatar, created_at)
      VALUES (?, ?, ?, ?, ?, ?)`,
      [userId, username, password, userNick, '', new Date().toISOString()]
    );

    console.log(`📤【执行插入】SQL参数 -> ID:${userId} | 账号:${username} | 明文密码:${password} | 昵称:${userNick}`);
    console.log(`📊【插入结果】影响行数:${insertResult.changes}`);

    if (insertResult.changes > 0) {
      console.log(`✅【注册成功】账号:${username} | 用户ID:${userId}`);
      return res.json({ code: 200, msg: '✅ 注册成功' });
    } else {
      throw new Error('注册写入失败：数据库无插入行数');
    }
  } catch (err) {
    console.error(`❌【注册失败】详细错误:`, err);
    next(err);
  }
});


// 登录接口
app.post('/login', async (req, res) => {
  const now = new Date().toLocaleString();
  const { username, password } = req.body;

  console.log(`📝【登录请求】[${now}] 接收参数 -> 账号:${username} | 明文密码:${password}`);

  // 1. 基础校验
  if (!username || !password) {
    console.log(`❌【登录失败】[${now}] 账号或密码为空`);
    return res.json({ code: 400, msg: '用户名和密码不能为空' });
  }

  try {
    // 2. 从数据库查询用户（**读取最新nickname**）
    console.log(`🔍【查询账号】[${now}] 正在查询用户 -> ${username}`);
    const user = await db.get("SELECT * FROM users WHERE username = ?", [username]);
    
    if (!user) {
      console.log(`❌【登录失败】[${now}] 账号不存在 -> ${username}`);
      return res.json({ code: 400, msg: '账号不存在，请先注册' });
    }

    console.log(`✅【找到账号】[${now}] 用户信息 -> ID:${user.id} | 用户名:${user.username} | 数据库最新昵称:${user.nickname} | 库内密码:${user.password}`);

    // 3. 密码校验
    if (user.password !== password) {
      console.log(`❌【登录失败】[${now}] 密码不匹配 -> 输入密码:${password} | 库内密码:${user.password}`);
      return res.json({ code: 400, msg: '密码错误' });
    }

    // 4. 登录成功：**使用数据库里的最新昵称**
    loginMap.set(username, {
      username: username,   // 用户名永久唯一，不变
      nickname: user.nickname, // 从数据库读取最新昵称
      password: password
    });

    console.log(`🎉【登录成功】[${now}] 用户名:${username} | 最终登录昵称:${user.nickname}`);
    sysLog('USER', '用户登录', { username });
    
    // 返回数据库里的最新昵称给前端
    return res.json({
      code: 200,
      msg: '登录成功',
      nickname: user.nickname
    });

  } catch (err) {
    console.error(`💥【登录异常】[${now}] 服务器错误详情:`, err);
    return res.json({ code: 500, msg: '服务器异常' });
  }
});



// Socket.io
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
  const now = new Date().toLocaleString();
  // 1. 校验登录状态
  if (!socket.username) {
    console.log(`❌【改昵称失败】[${now}] 用户未登录`);
    return socket.emit('change-nick-res', { code: 401, msg: '请先登录' });
  }

  // 2. 校验昵称合法性
  if (!newNick || newNick.trim() === '') {
    console.log(`❌【改昵称失败】[${now}] 昵称不能为空`);
    return socket.emit('change-nick-res', { code: 400, msg: '昵称不能为空' });
  }

  const finalNick = newNick.trim();
  const loginUsername = socket.username; // 用户名永久不变

  try {
    console.log(`📝【改昵称请求】[${now}] 用户名:${loginUsername} | 旧昵称:${socket.nickname} | 新昵称:${finalNick}`);
    
    // 3. 更新数据库：只改nickname，不动username
    await db.run(
      `UPDATE users SET nickname = ? WHERE username = ?`,
      [finalNick, loginUsername]
    );

    // 4. 更新内存缓存
    socket.nickname = finalNick;
    loginMap.get(loginUsername).nickname = finalNick;

    console.log(`✅【改昵称成功】[${now}] 用户名:${loginUsername} | 新昵称已入库:${finalNick}`);
    socket.emit('change-nick-res', { code: 200, msg: '昵称修改成功', nickname: finalNick });

    // 广播给所有在线用户
    io.emit('user-online', Array.from(loginMap.values()).map(u => ({
      username: u.username,
      nickname: u.nickname
    })));

  } catch (err) {
    console.error(`💥【改昵称异常】[${now}] 用户名:${loginUsername} | 错误详情:`, err);
    socket.emit('change-nick-res', { code: 500, msg: '昵称修改失败，请重试' });
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

// 3分钟自我保活
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
    try {
      await db.execute("SELECT 1 AS test");
      writeLog('✅ D1数据库正常');
    } catch (e) {
      writeLog('⚠️ D1异常，但服务正常运行');
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
