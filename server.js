const express = require('express');
const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const { Server } = require('socket.io');
const crypto = require('crypto');
const readline = require('readline');
const app = express();

// 加载SSL证书（宝塔自动生成的路径，直接用）
const sslOptions = {
  key: fs.readFileSync('/www/server/panel/vhost/cert/im6.cc.cd/privkey.pem'),
  cert: fs.readFileSync('/www/server/panel/vhost/cert/im6.cc.cd/fullchain.pem')
};

// 创建HTTPS服务，端口仍然是6500
const server = https.createServer(sslOptions, app);

// 👇 你的原版CORS配置完全不动
const io = new Server(server, {
  cors: { 
    origin: "https://im6.cc.cd", 
    methods: ["GET", "POST"], 
    credentials: true 
  },
  transports: ['websocket', 'polling'],
  pingTimeout: 30000,
  pingInterval: 10000,
  maxHttpBufferSize: 10 * 1024 * 1024,
  allowEIO3: true
});

const PWD_FILE = path.join(__dirname, 'admin.pwd');
const PRI_KEY_FILE = path.join(__dirname, 'server.pri');
const PUB_KEY_FILE = path.join(__dirname, 'server.pub');
const MODE_FILE = path.join(__dirname, 'key.mode');
const USERS_FILE = path.join(__dirname, 'users.json');
let ADMIN_PWD = '';
let SERVER_PRIVATE_KEY = '';
let SERVER_PUBLIC_KEY = '';
let KEY_MODE = 'auto';
const KEY_ALG = 'secp256k1';
const HEARTBEAT_INTERVAL = 30000;
let userMap = new Map();
let waitingUsers = new Set();
let loginMap = new Map();
let userSessionMap = new Map();
let keepAliveMap = new Map();
const KEEP_ALIVE_EXPIRE = 24 * 60 * 60 * 1000;
const KEEP_ALIVE_CHECK_INTERVAL = 60 * 1000;
const UNLOGGED_CLEAN_INTERVAL = 30000;
const REDIS_EXPIRE = 7200;
let roomMem = new Map();
let offlineMsgMem = new Map();
const PORT = 6500;
let ENABLE_VIRTUAL_USERS = true;
const VIRTUAL_USERS = ['小甜甜', '小美', '小雅', '静静'];
const VIRTUAL_USER_ID_PREFIX = 'robot_';
const MATCH_TIMEOUT = 30000;
let userMatchTimer = new Map();

function getVirtualUserId(name) {
  return `${VIRTUAL_USER_ID_PREFIX}${name.toLowerCase()}`;
}

function loadPwd() {
  try {
    if (fs.existsSync(PWD_FILE)) {
      ADMIN_PWD = fs.readFileSync(PWD_FILE, 'utf8').trim();
      if (!ADMIN_PWD) ADMIN_PWD = '123456';
    } else {
      fs.writeFileSync(PWD_FILE, '123456');
      ADMIN_PWD = '123456';
    }
  } catch (e) {
    ADMIN_PWD = '123456';
    fs.writeFileSync(PWD_FILE, ADMIN_PWD);
  }
}

function loadUsers() {
  try {
    if (fs.existsSync(USERS_FILE)) {
      const userData = fs.readFileSync(USERS_FILE, 'utf8').trim();
      const users = userData ? JSON.parse(userData) : [];
      users.forEach(u => loginMap.set(u.username, u.password));
    } else {
      fs.writeFileSync(USERS_FILE, JSON.stringify([]), 'utf8');
    }
  } catch (e) {
    fs.writeFileSync(USERS_FILE, JSON.stringify([]), 'utf8');
    loginMap.clear();
  }
}

function saveUser(username, password) {
  try {
    let users = [];
    if (fs.existsSync(USERS_FILE)) {
      const userData = fs.readFileSync(USERS_FILE, 'utf8').trim();
      users = userData ? JSON.parse(userData) : [];
    }
    users.push({ username, password });
    fs.writeFileSync(USERS_FILE, JSON.stringify(users, null, 2), 'utf8');
    loginMap.set(username, password);
    return true;
  } catch (e) {
    return false;
  }
}

function deleteUser(username, adminPwd) {
  try {
    if (adminPwd !== ADMIN_PWD) return { code: 403, msg: '管理员密码错误' };
    if (!loginMap.has(username)) return { code: 404, msg: '用户不存在' };
    let users = [];
    if (fs.existsSync(USERS_FILE)) {
      const userData = fs.readFileSync(USERS_FILE, 'utf8').trim();
      users = userData ? JSON.parse(userData) : [];
    }
    const newUsers = users.filter(u => u.username !== username);
    fs.writeFileSync(USERS_FILE, JSON.stringify(newUsers, null, 2), 'utf8');
    loginMap.delete(username);
    userSessionMap.delete(username);
    userMap.forEach((user, socketId) => {
      if (user.username === username) {
        user.socket?.emit('user-deleted', { msg: '账号已删除' });
        user.socket?.disconnect(true);
        userMap.delete(socketId);
        waitingUsers.delete(socketId);
        keepAliveMap.delete(socketId);
      }
    });
    return { code: 200, msg: '删除成功' };
  } catch (e) {
    return { code: 500, msg: '删除失败' };
  }
}

function loadKeys() {
  try {
    if (fs.existsSync(PRI_KEY_FILE)) SERVER_PRIVATE_KEY = fs.readFileSync(PRI_KEY_FILE, 'utf8').trim();
    if (fs.existsSync(PUB_KEY_FILE)) SERVER_PUBLIC_KEY = fs.readFileSync(PUB_KEY_FILE, 'utf8').trim();
    if (fs.existsSync(MODE_FILE)) {
      KEY_MODE = fs.readFileSync(MODE_FILE, 'utf8').trim();
      if (!['auto', 'manual'].includes(KEY_MODE)) KEY_MODE = 'auto';
    } else {
      fs.writeFileSync(MODE_FILE, 'auto');
      KEY_MODE = 'auto';
    }
  } catch (e) {
    SERVER_PRIVATE_KEY = '';
    SERVER_PUBLIC_KEY = '';
  }
}

function generateKeys(force = false) {
  if (KEY_MODE === 'manual' && !force) return;
  try {
    const ecdh = crypto.createECDH(KEY_ALG);
    ecdh.generateKeys();
    SERVER_PUBLIC_KEY = ecdh.getPublicKey('hex');
    SERVER_PRIVATE_KEY = ecdh.getPrivateKey('hex');
    fs.writeFileSync(PUB_KEY_FILE, SERVER_PUBLIC_KEY);
    fs.writeFileSync(PRI_KEY_FILE, SERVER_PRIVATE_KEY);
  } catch (e) {}
}

function switchKeyMode(mode) {
  if (!['auto', 'manual'].includes(mode)) return;
  KEY_MODE = mode;
  fs.writeFileSync(MODE_FILE, KEY_MODE);
  if (mode === 'auto') generateKeys(true);
}

function createMatchRoom(userA, userB) {
  const roomId = `room_${Date.now()}_${Math.floor(Math.random() * 10000)}`;
  roomMem.set(roomId, { userA, userB, userALeft: false, userBLeft: false, createTime: Date.now() });
  setTimeout(() => roomMem.delete(roomId), REDIS_EXPIRE * 1000);
  return roomId;
}

function saveOfflineMsg(toUserId, msg) {
  if (!offlineMsgMem.has(toUserId)) offlineMsgMem.set(toUserId, []);
  offlineMsgMem.get(toUserId).push(msg);
  setTimeout(() => {
    const ms = offlineMsgMem.get(toUserId) || [];
    const f = ms.filter(m => m.timestamp + REDIS_EXPIRE * 1000 > Date.now());
    f.length ? offlineMsgMem.set(toUserId, f) : offlineMsgMem.delete(toUserId);
  }, REDIS_EXPIRE * 1000);
}

function pushOfflineMsg(socket, userId) {
  const ms = offlineMsgMem.get(userId) || [];
  ms.forEach(m => socket.emit("OFFLINE_MESSAGE", m));
  offlineMsgMem.delete(userId);
}

function checkReconnectValid(userId, roomId) {
  const room = roomMem.get(roomId);
  if (!room) return { success: false, reason: '会话不存在' };
  const isA = room.userA === userId;
  const isB = room.userB === userId;
  if (!isA && !isB) return { success: false, reason: '非会话成员' };
  const opp = isA ? room.userB : room.userA;
  const oppLeft = isA ? room.userBLeft : room.userALeft;
  if (oppLeft) return { success: false, reason: '对方已离开' };
  if (Date.now() - room.createTime > REDIS_EXPIRE * 1000) {
    roomMem.delete(roomId);
    return { success: false, reason: '会话过期' };
  }
  return { success: true, opponent: opp };
}

function markUserLeave(userId, roomId) {
  const room = roomMem.get(roomId);
  if (!room) return;
  if (room.userA === userId) room.userALeft = true;
  if (room.userB === userId) room.userBLeft = true;
  const opp = room.userA === userId ? room.userB : room.userA;
  const os = Array.from(userMap.values()).find(u => u.username === opp);
  os && os.socket.emit("USER_LEFT");
}

function resetRoomExpire(roomId) {
  const r = roomMem.get(roomId);
  if (r) r.createTime = Date.now();
}

function verifyUserLogin(userId, token) {
  return loginMap.has(userId) && token === userId;
}

function updateUserSocket(oldName, sock, sid) {
  userMap.forEach((u, s) => {
    if (u.username === oldName && s !== sid) {
      userMap.delete(s);
      waitingUsers.delete(s);
      keepAliveMap.delete(s);
      if (userMatchTimer.has(s)) { clearTimeout(userMatchTimer.get(s)); userMatchTimer.delete(s); }
    }
  });
  const nu = { id: sid, socket: sock, username: oldName, isMatched: false, partner: null, roomId: null, heartbeatStatus: true, lastActive: Date.now(), lastKeepAlive: Date.now() };
  userMap.set(sid, nu);
  userSessionMap.set(oldName, sid);
  return nu;
}

function assignVirtualUser(sid) {
  const u = userMap.get(sid);
  if (!u || !u.socket || !u.socket.connected || u.isMatched || !waitingUsers.has(sid)) return;
  const vn = VIRTUAL_USERS[Math.floor(Math.random() * VIRTUAL_USERS.length)];
  const vid = getVirtualUserId(vn);
  const rid = createMatchRoom(u.username, vn);
  u.partner = vid;
  u.isMatched = true;
  u.roomId = rid;
  waitingUsers.delete(sid);
  userMap.set(sid, u);
  u.lastKeepAlive = Date.now();
  keepAliveMap.set(sid, { partnerId: vid, expireTime: Date.now() + KEEP_ALIVE_EXPIRE });
  u.socket.emit('match-found', { partnerId: vid, partnerName: vn, selfId: sid, roomId: rid });
  if (userMatchTimer.has(sid)) { clearTimeout(userMatchTimer.get(sid)); userMatchTimer.delete(sid); }
}

function autoJoinMatchPool(sid) {
  const u = userMap.get(sid);
  if (!u || !u.socket || !u.socket.connected || !u.username || !loginMap.has(u.username) || userSessionMap.get(u.username) !== u.id || u.isMatched || waitingUsers.has(sid)) return;
  u.heartbeatStatus = true;
  u.lastActive = Date.now();
  waitingUsers.add(sid);
  userMap.set(sid, u);
  if (ENABLE_VIRTUAL_USERS) {
    const t = setTimeout(() => assignVirtualUser(sid), MATCH_TIMEOUT);
    userMatchTimer.set(sid, t);
  }
  tryMatch();
}

function clearAllChatData() {
  userMap.forEach(u => {
    if (u.username && loginMap.has(u.username) && userSessionMap.get(u.username) === u.id)
      u.socket?.emit('clear-chat-record', { msg: '已清空聊天记录' });
  });
}

function stopChat(uid, isInitiative = true) {
  const me = userMap.get(uid);
  if (!me || !me.username || !loginMap.has(me.username) || userSessionMap.get(me.username) !== uid || !me.partner) return;
  const pt = userMap.get(me.partner);
  if (pt && pt.username && loginMap.has(pt.username) && userSessionMap.get(pt.username) === pt.id && pt.socket?.connected) {
    pt.partner = null;
    pt.isMatched = false;
    pt.socket.emit('partner-leave');
    pt.socket.emit('match-end', { info: '对方已离开' });
    autoJoinMatchPool(pt.id);
  }
  me.partner = null;
  me.isMatched = false;
  me.socket.emit('match-end', { info: isInitiative ? '已断开' : '聊天已结束' });
  keepAliveMap.delete(uid);
  if (me.partner) keepAliveMap.delete(me.partner);
  if (me.roomId) {
    roomMem.delete(me.roomId);
    offlineMsgMem.delete(me.username);
    me.roomId = null;
  }
  if (userMatchTimer.has(uid)) { clearTimeout(userMatchTimer.get(uid)); userMatchTimer.delete(uid); }
  autoJoinMatchPool(me.id);
}

function tryMatch() {
  const valid = Array.from(waitingUsers).map(id => userMap.get(id)).filter(u =>
    u && u.socket && u.socket.connected && !u.partner && u.username &&
    loginMap.has(u.username) && userSessionMap.get(u.username) === u.id &&
    !VIRTUAL_USERS.includes(u.username)
  );
  Array.from(waitingUsers).forEach(id => {
    const u = userMap.get(id);
    if (!u || !u.socket || !u.socket.connected || u.partner || !u.username || !loginMap.has(u.username) || userSessionMap.get(u.username) !== id) {
      waitingUsers.delete(id);
      if (userMatchTimer.has(id)) { clearTimeout(userMatchTimer.get(id)); userMatchTimer.delete(id); }
    }
  });
  if (valid.length < 2) return;
  const u1 = valid[0], u2 = valid[1];
  if (u1.id === u2.id) return;
  if (userMatchTimer.has(u1.id)) { clearTimeout(userMatchTimer.get(u1.id)); userMatchTimer.delete(u1.id); }
  if (userMatchTimer.has(u2.id)) { clearTimeout(userMatchTimer.get(u2.id)); userMatchTimer.delete(u2.id); }
  u1.partner = u2.id; u2.partner = u1.id; u1.isMatched = true; u2.isMatched = true;
  waitingUsers.delete(u1.id); waitingUsers.delete(u2.id);
  const rid = createMatchRoom(u1.username, u2.username);
  u1.roomId = rid; u2.roomId = rid;
  userMap.set(u1.id, u1); userMap.set(u2.id, u2);
  u1.socket.emit('match-found', { partnerId: u2.id, partnerName: u2.username, selfId: u1.id, roomId: rid });
  u2.socket.emit('match-found', { partnerId: u1.id, partnerName: u1.username, selfId: u2.id, roomId: rid });
  u1.lastKeepAlive = Date.now(); u2.lastKeepAlive = Date.now();
  keepAliveMap.set(u1.id, { partnerId: u2.id, expireTime: Date.now() + KEEP_ALIVE_EXPIRE });
  keepAliveMap.set(u2.id, { partnerId: u1.id, expireTime: Date.now() + KEEP_ALIVE_EXPIRE });
}

function pushAd(c) {
  if (!c) return;
  userMap.forEach(u => {
    if (u.socket && u.socket.connected && u.username && loginMap.has(u.username) && userSessionMap.get(u.username) === u.id)
      u.socket.emit('ad_push', { content: c });
  });
}

function checkPartnerStatus(uid) {
  const me = userMap.get(uid);
  if (!me || !me.username || !loginMap.has(me.username) || userSessionMap.get(me.username) !== uid || !me.partner) {
    me.socket.emit('partner-status', { isOnline: false, info: '无匹配' });
    return;
  }
  if (me.partner.startsWith(VIRTUAL_USER_ID_PREFIX)) {
    const n = me.partner.replace(VIRTUAL_USER_ID_PREFIX, '').replace(/^\w/, c => c.toUpperCase());
    me.socket.emit('partner-status', { isOnline: true, info: '对方在线', partnerId: me.partner, partnerName: n });
    return;
  }
  const pt = userMap.get(me.partner);
  if (pt && pt.username && loginMap.has(pt.username) && userSessionMap.get(pt.username) === pt.id && pt.socket?.connected) {
    me.socket.emit('partner-status', { isOnline: true, info: '在线', partnerId: pt.id, partnerName: pt.username });
  } else {
    me.partner = null; me.isMatched = false;
    me.socket.emit('partner-status', { isOnline: false, info: '对方已离开' });
    me.socket.emit('partner-leave');
    autoJoinMatchPool(me.id);
  }
}

function startKeepAliveCheck() {
  setInterval(() => {
    const now = Date.now();
    keepAliveMap.forEach((inf, uid) => {
      const u = userMap.get(uid);
      const pid = inf.partnerId;
      if (pid.startsWith(VIRTUAL_USER_ID_PREFIX)) return;
      const p = userMap.get(pid);
      if (!u || !p || !u.username || !p.username || !loginMap.has(u.username) || !loginMap.has(p.username) || userSessionMap.get(u.username) !== uid || userSessionMap.get(p.username) !== pid || !u.socket?.connected || !p.socket?.connected) {
        keepAliveMap.delete(uid); keepAliveMap.delete(pid);
        u?.socket?.emit('partner-leave'); p?.socket?.emit('partner-leave');
        if(u) autoJoinMatchPool(uid); if(p) autoJoinMatchPool(pid);
        return;
      }
      if (now > inf.expireTime) {
        u.socket.emit('keep-alive-expire', { info: '24小时到期' });
        p.socket.emit('keep-alive-expire', { info: '24小时到期' });
        u.socket.emit('partner-leave'); p.socket.emit('partner-leave');
        stopChat(uid, false); stopChat(pid, false);
        keepAliveMap.delete(uid); keepAliveMap.delete(pid);
        return;
      }
      const ut = now - (u.lastKeepAlive || 0) > 5 * 60 * 1000;
      const pt = now - (p.lastKeepAlive || 0) > 5 * 60 * 1000;
      if (ut || pt) {
        u.socket.emit('match-expire', { info: '长时间未响应' });
        p.socket.emit('match-expire', { info: '长时间未响应' });
        u.socket.emit('partner-leave'); p.socket.emit('partner-leave');
        stopChat(uid, false);
        keepAliveMap.delete(uid); keepAliveMap.delete(pid);
        return;
      }
      u.heartbeatStatus = true; p.heartbeatStatus = true;
      u.lastActive = now; p.lastActive = now;
      if (u.roomId) resetRoomExpire(u.roomId);
    });
  }, KEEP_ALIVE_CHECK_INTERVAL);
}

const rl = readline.createInterface({ input: process.stdin, output: process.stdout, prompt: '>' });

function showMenu() {
  console.log(`
1查看在线 2注册列表 3虚拟开关 6清空记录 7断开全部 9删用户
a公钥 b私钥 c改密 d刷新密钥 x自动密钥 y手动密钥 0退出
ad内容 推送广告
`);
  rl.prompt();
}

rl.on('line', i => {
  const c = i.trim();
  if (c.toLowerCase().startsWith('ad ')) { pushAd(c.slice(3).trim()); rl.prompt(); return; }
  if (c === '9') {
    const users = Array.from(loginMap.keys());
    if (!users.length) { console.log('无用户'); rl.prompt(); return; }
    users.forEach((n, j) => console.log(j+1+'. '+n));
    rl.question('序号>', i => {
      const idx = +i -1;
      if (idx <0 || idx >= users.length) { rl.prompt(); return; }
      const name = users[idx];
      rl.question('管理员密码>', pwd => {
        const r = deleteUser(name, pwd.trim());
        console.log(r.code === 200 ? '✅成功' : '❌失败：'+r.msg);
        rl.prompt();
      });
    });
    return;
  }
  switch(c.toLowerCase()){
    case '1': userMap.forEach((u,id)=>{console.log(id, u.username||'未登录');}); break;
    case '2': Array.from(loginMap.keys()).forEach((n,i)=>console.log(i+1+'.'+n)); break;
    case '3': ENABLE_VIRTUAL_USERS=!ENABLE_VIRTUAL_USERS; console.log('虚拟用户：'+(ENABLE_VIRTUAL_USERS?'开':'关')); break;
    case '6': clearAllChatData(); break;
    case '7': case '8': userMap.forEach(u=>{if(u.partner)stopChat(u.id,false);}); break;
    case 'a': console.log('公钥：\n'+SERVER_PUBLIC_KEY); break;
    case 'b': console.log('私钥：\n'+SERVER_PRIVATE_KEY); break;
    case 'c': rl.question('旧密>',o=>{if(o!==ADMIN_PWD){console.log('错');rl.prompt();return;}
      rl.question('新密>',n=>{ADMIN_PWD=n.trim();fs.writeFileSync(PWD_FILE,ADMIN_PWD);console.log('✅改密成功');rl.prompt();});
    });return;
    case 'd': generateKeys(true); break;
    case 'x': switchKeyMode('auto'); break;
    case 'y': switchKeyMode('manual'); break;
    case '0':
      userMap.forEach(u=>{u.socket?.disconnect(true);});
      process.exit(0);
      break;
  }
  rl.prompt();
});

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));
app.use(express.static(__dirname));

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));

app.post('/_hidden_change_key', (req, res) => {
  const { startupPassword, pubkey } = req.body;
  if (startupPassword !== ADMIN_PWD) return res.json({ ok: false });
  if (pubkey && pubkey.length >= 60) {
    switchKeyMode('manual');
    SERVER_PUBLIC_KEY = pubkey.trim();
    fs.writeFileSync(PUB_KEY_FILE, SERVER_PUBLIC_KEY);
    return res.json({ ok: true });
  }
  generateKeys(true);
  res.json({ ok: true });
});

app.post('/register', (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) return res.json({ code: 400, msg: '不能为空' });
  if (username.length < 2 || username.length > 20) return res.json({ code: 400, msg: '用户名2-20位' });
  if (VIRTUAL_USERS.includes(username)) return res.json({ code: 400, msg: '不可用' });
  if (password.length < 6) return res.json({ code: 400, msg: '密码至少6位' });
  if (loginMap.has(username)) return res.json({ code: 400, msg: '已存在' });
  saveUser(username, password);
  res.json({ code: 200, msg: '注册成功' });
});

app.post('/login', (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) return res.json({ code: 400, msg: '不能为空' });
  if (VIRTUAL_USERS.includes(username)) return res.json({ code: 400, msg: '不可用' });
  if (loginMap.has(username) && loginMap.get(username) === password)
    return res.json({ code: 200, msg: '登录成功' });
  res.json({ code: 400, msg: '账号或密码错误' });
});

io.on('connection', socket => {
  const sid = socket.id;
  const user = {
    id: sid, socket: socket, username: '', partner: null, isMatched: false,
    lastActive: Date.now(), heartbeatStatus: true, aesKey: '', lastKeepAlive: Date.now(), roomId: null
  };
  userMap.set(sid, user);
  socket.emit('server-init', { pubkey: SERVER_PUBLIC_KEY, heartbeatInterval: HEARTBEAT_INTERVAL });

  const unloggedTimer = setInterval(() => {
    if (!user.username || !loginMap.has(user.username) || userSessionMap.get(user.username) !== sid) {
      socket.disconnect(true); userMap.delete(sid); clearInterval(unloggedTimer);
    } else clearInterval(unloggedTimer);
  }, UNLOGGED_CLEAN_INTERVAL);

  socket.on('checkLogin', d => {
    const { userId, token } = d;
    if (VIRTUAL_USERS.includes(userId)) { socket.emit('notLogin'); return; }
    const ok = userId && token === userId && loginMap.has(userId);
    if (!ok) { socket.emit('notLogin'); return; }
    user.username = userId;
    user.heartbeatStatus = true;
    user.lastActive = Date.now();
    userMap.set(sid, user);
    if (userSessionMap.get(userId) !== sid) {
      const old = userSessionMap.get(userId);
      if (old) {
        const ou = userMap.get(old);
        if (ou && ou.socket?.connected) {
          ou.socket.emit('kick-out', { msg: '账号在别处登录' });
          ou.username = ''; ou.isMatched = false; ou.partner = null; ou.roomId = null;
          userMap.set(old, ou); waitingUsers.delete(old); keepAliveMap.delete(old);
          if (userMatchTimer.has(old)) { clearTimeout(userMatchTimer.get(old)); userMatchTimer.delete(old); }
        }
      }
      userSessionMap.set(userId, sid);
    }
    pushOfflineMsg(socket, userId);
    socket.emit('loginSuccess');
    autoJoinMatchPool(sid);
  });

  socket.on('HEARTBEAT', () => {
    if (user.username && loginMap.has(user.username) && userSessionMap.get(user.username) === sid) {
      user.lastActive = Date.now();
      user.heartbeatStatus = true;
      socket.emit('heartbeat-ack');
      if (user.roomId) resetRoomExpire(user.roomId);
    }
  });

  socket.on('user-online', d => {
    const name = d?.username?.trim();
    if (VIRTUAL_USERS.includes(name)) { socket.emit('invalid-username'); return; }
    if (name && name.length >=2 && name.length <=20 && loginMap.has(name)) {
      if (userSessionMap.has(name)) {
        const old = userSessionMap.get(name);
        const ou = userMap.get(old);
        if (ou && ou.socket?.connected) {
          ou.socket.emit('kick-out', { msg: '账号在别处登录' });
          ou.username = ''; ou.isMatched = false; ou.partner = null; ou.roomId = null;
          userMap.set(old, ou); waitingUsers.delete(old); keepAliveMap.delete(old);
          if (userMatchTimer.has(old)) { clearTimeout(userMatchTimer.get(old)); userMatchTimer.delete(old); }
        }
      }
      user.username = name;
      user.heartbeatStatus = true;
      user.lastActive = Date.now();
      userMap.set(sid, user);
      userSessionMap.set(name, sid);
      socket.emit('username-set-success');
      autoJoinMatchPool(sid);
    } else socket.emit('invalid-username');
  });

  socket.on('match-chat', () => {
    if (user.username && loginMap.has(user.username) && userSessionMap.get(user.username) !== sid)
      return socket.emit('kick-out');
    if (!user.username) return socket.emit('match-end', { info: '请登录' });
    if (user.isMatched) stopChat(sid, false);
    if (waitingUsers.has(sid)) return socket.emit('match-tip', { info: '已在队列' });
    waitingUsers.add(sid);
    const t = setTimeout(() => assignVirtualUser(sid), MATCH_TIMEOUT);
    userMatchTimer.set(sid, t);
    tryMatch();
  });

  socket.on('stop-chat', () => {
    if (user.username && loginMap.has(user.username) && userSessionMap.get(user.username) !== sid)
      return socket.emit('kick-out');
    if (userMatchTimer.has(sid)) { clearTimeout(userMatchTimer.get(sid)); userMatchTimer.delete(sid); }
    stopChat(sid, true);
  });

  socket.on('check-partner', () => {
    if (user.username && loginMap.has(user.username) && userSessionMap.get(user.username) !== sid)
      return socket.emit('kick-out');
    checkPartnerStatus(sid);
  });

  socket.on('user-pubkey', up => {
    if (!user.username || !loginMap.has(user.username) || !up || !SERVER_PRIVATE_KEY) return;
    try {
      const ec = crypto.createECDH(KEY_ALG);
      ec.setPrivateKey(SERVER_PRIVATE_KEY, 'hex');
      const sk = ec.computeSecret(up, 'hex');
      user.aesKey = crypto.createHash('sha256').update(sk).digest('hex').slice(0,32);
    } catch(e){}
  });

  socket.on('send-msg', d => {
    if (user.username && loginMap.has(user.username) && userSessionMap.get(user.username) !== sid)
      return socket.emit('kick-out');
    if (!user.username || !user.isMatched || !user.partner || !d)
      return socket.emit('msg-fail', { info: '未匹配' });
    const tv = userMap.get(user.partner);
    const tvn = tv?.username;
    if (user.partner.startsWith(VIRTUAL_USER_ID_PREFIX)) {
      if (d.type === 'text' && d.content) {
        const rep = ["嗯嗯～","原来是这样呀","你说得对","哈哈真有趣","我也是","后来呢？"];
        const r = rep[Math.floor(Math.random()*rep.length)];
        setTimeout(() => {
          socket.emit('new-msg', {
            content: r, type: 'text', burn: d.burn||false, receipt: d.receipt||false,
            msgId: 'v_'+Date.now(), fromId: user.partner, fromName: user.partner.replace(VIRTUAL_USER_ID_PREFIX,'')
          });
          if (d.receipt && d.msgId) socket.emit('msg-read', { msgId: d.msgId });
        }, 300+Math.random()*500);
      } else if (d.receipt && d.msgId) socket.emit('msg-read', { msgId: d.msgId });
      return;
    }
    if (tv && tvn && loginMap.has(tvn) && userSessionMap.get(tvn) === tv.id && tv.socket?.connected) {
      tv.socket.emit('new-msg', {
        content: d.content, data: d.data, type: d.type||'text', burn: d.burn||false,
        receipt: d.receipt||false, msgId: d.msgId||'', fromId: sid, fromName: user.username
      });
    } else {
      saveOfflineMsg(tvn||d.toId, {
        fromId: user.username, type: d.type||'text', content: d.content, data: d.data,
        receipt: d.receipt||false, burn: d.burn||false, msgId: d.msgId||'', timestamp: Date.now()
      });
      socket.emit('msg-fail', { info: '对方离线，已存离线消息' });
    }
  });

  socket.on('msg-read-confirm', d => {
    if (!user.username || !d.msgId || !d.toId) return;
    const tu = userMap.get(d.toId);
    if (tu && tu.username && loginMap.has(tu.username) && userSessionMap.get(tu.username) === tu.id && tu.socket?.connected)
      tu.socket.emit('msg-read', { msgId: d.msgId });
  });

  socket.on('keep-alive', () => {
    if (user.username && loginMap.has(user.username) && userSessionMap.get(user.username) !== sid)
      return socket.emit('kick-out');
    if (!user.isMatched || !user.partner) return socket.emit('keep-alive-ack', { success: false });
    if (user.partner.startsWith(VIRTUAL_USER_ID_PREFIX)) {
      user.lastKeepAlive = Date.now();
      keepAliveMap.set(sid, { partnerId: user.partner, expireTime: Date.now() + KEEP_ALIVE_EXPIRE });
      return socket.emit('keep-alive-ack', { success: true });
    }
    const pt = userMap.get(user.partner);
    if (!pt || !pt.username || !loginMap.has(pt.username) || userSessionMap.get(pt.username) !== pt.id) {
      socket.emit('keep-alive-ack', { success: false });
      socket.emit('partner-leave');
      stopChat(sid, false);
      return;
    }
    user.lastKeepAlive = Date.now();
    pt.lastKeepAlive = Date.now();
    keepAliveMap.set(sid, { partnerId: pt.id, expireTime: Date.now() + KEEP_ALIVE_EXPIRE });
    keepAliveMap.set(pt.id, { partnerId: sid, expireTime: Date.now() + KEEP_ALIVE_EXPIRE });
    if (user.roomId) resetRoomExpire(user.roomId);
    socket.emit('keep-alive-ack', { success: true });
  });

  socket.on('RECONNECT', d => {
    const { userId, roomId } = d;
    if (!userId || !roomId || VIRTUAL_USERS.includes(userId) || userId !== user.username || userSessionMap.get(userId) !== sid)
      return socket.emit('RECONNECT_RESULT', { success: false });
    const cr = checkReconnectValid(userId, roomId);
    socket.emit('RECONNECT_RESULT', cr);
    if (cr.success) {
      user.isMatched = true; user.roomId = roomId;
      const room = roomMem.get(roomId);
      const opp = room.userA === userId ? room.userB : room.userA;
      user.partner = VIRTUAL_USERS.includes(opp) ? getVirtualUserId(opp) : Array.from(userMap.values()).find(x=>x.username===opp)?.id;
      userMap.set(sid, user);
      pushOfflineMsg(socket, userId);
    }
  });

  socket.on('LEAVE', d => {
    const { userId, roomId } = d;
    if (!userId || !roomId || userId !== user.username || userSessionMap.get(userId) !== sid)
      return socket.emit('LEAVE_RESULT', { success: false });
    markUserLeave(userId, roomId);
    if (userMatchTimer.has(sid)) { clearTimeout(userMatchTimer.get(sid)); userMatchTimer.delete(sid); }
    stopChat(sid, true);
    socket.emit('LEAVE_RESULT', { success: true });
  });

  socket.on('disconnect', () => {
    if (userMatchTimer.has(sid)) { clearTimeout(userMatchTimer.get(sid)); userMatchTimer.delete(sid); }
    if (user.username && loginMap.has(user.username)) {
      if (userSessionMap.get(user.username) === sid) userSessionMap.delete(user.username);
      if (user.partner && !user.partner.startsWith(VIRTUAL_USER_ID_PREFIX)) {
        const pt = userMap.get(user.partner);
        if (pt && pt.username && loginMap.has(pt.username)) pt.socket?.emit('partner-leave');
      }
      keepAliveMap.delete(sid);
      if (user.partner) keepAliveMap.delete(user.partner);
      setTimeout(() => userMap.delete(sid), 180000);
    } else userMap.delete(sid);
  });
});

loadPwd();
loadUsers();
loadKeys();
if (!SERVER_PUBLIC_KEY || !SERVER_PRIVATE_KEY) generateKeys(true);
startKeepAliveCheck();

server.listen(PORT, '0.0.0.0', () => {
  console.log('✅ HTTPS 服务启动成功 端口：6500');
  console.log('✅ 证书已加载');
  console.log('✅ 端到端加密正常');
  console.log('✅ 前端已绑定');
  showMenu();
});

process.on('uncaughtException', e=>{});
process.on('unhandledRejection', r=>{});
process.on('SIGINT', ()=>process.exit(0));

app.use((req,res)=>res.status(404).json({code:404}));
