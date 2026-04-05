const express = require('express');
const crypto = require('crypto');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = 3000;

app.use(express.json({ limit: '10mb' })); 
app.use(express.static(path.join(__dirname, 'public')));
app.use("/seo", express.static(path.join(__dirname, "public", "seo")));

// In-memory databases
const DB_FILE = path.join(__dirname, 'db.json');
let db = {
    users: [], sessions: {}, games: [], shopItems: [], groups: [], cityPlots: [], datastores: {},
    globalChat: [], toolboxItems: [], // NEW
    notifications: [],
    moderation: { bans: {}, ipBans: [], warnings: {} },  // <-- ADD THIS LINE
};

let chatActivity = {}; // Tracks timestamps for spam { userId: [timestamps] }
let chatSuspensions = {}; // Tracks suspensions { userId: unbanTimestamp }

let activeEditors = {}; 
let activePlayers = {}; 
let onlineUsers = {};   
let gameChats = {}; // { [gameId]: [messages] }
let gameChatActivity = {}; // { [gameId_userId]: [timestamps] }
let gameChatSuspensions = {}; // { [gameId_userId]: unbanTimestamp }
let adminAuth = { attempts: 0, lockoutUntil: 0 };
// Load existing DB if available & migrate data
if (fs.existsSync(DB_FILE)) {



if (!db.datastores) db.datastores = {};
if (!db.notifications) db.notifications = [];
if (!db.moderation) db.moderation = { bans: {}, ipBans: [], warnings: {} }; // <-- ADD THIS LINE
    try {
        const loaded = JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
        db = { ...db, ...loaded };
        
        if (!db.shopItems) db.shopItems = [];
if (!db.moderation) db.moderation = { bans: {}, ipBans: [], warnings: {} };
        if (!db.groups) db.groups = [];
if (!db.sounds) db.sounds = [];

        db.users.forEach(u => { 
            if (!u.followers) u.followers = []; 
if (typeof u.createdAt === 'undefined') u.createdAt = 0;
            if (!u.friends) u.friends = [];
            if (!u.friendRequests) u.friendRequests = [];
            if (!u.color) u.color = '#e74c3c';
if (!u.toolboxInventory) u.toolboxInventory = [];
            if (!u.recentlyPlayed) u.recentlyPlayed = [];
            if (!u.badges) u.badges = [];
            if (!u.messages) u.messages = [];
            if (!u.inventory) u.inventory = [];

// Add this right after parsing db.json
if (typeof db.lastUserIdNum === 'undefined') {
    db.lastUserIdNum = 0;
    // Retroactively assign sequential IDs to existing users
    db.users.forEach(u => {
        if (typeof u.userIdNum === 'undefined') {
            db.lastUserIdNum++;
            u.userIdNum = db.lastUserIdNum;
        }
    });
}

            if (!u.bookmarks) u.bookmarks = []; 
            if (typeof u.equipped === 'undefined') u.equipped = null;
            if (typeof u.primaryGroupId === 'undefined') u.primaryGroupId = null; 
            if (typeof u.coins === 'undefined') u.coins = 0; // Migrate coins to backend
if (typeof u.lastSpinDate === 'undefined') u.lastSpinDate = 0; // NEW: Lucky Spin Tracker

if (typeof u.lastPlayDate === 'undefined') u.lastPlayDate = 0;
            if (typeof u.cityData === 'undefined') u.cityData = null; // NEW: Track if user is in Sculpt City

if (typeof u.loginStreak === 'undefined') u.loginStreak = 0;
            if (typeof u.lastLoginDate === 'undefined') u.lastLoginDate = 0;
            if (typeof u.playStreak === 'undefined') u.playStreak = 0;
            if (typeof u.lastPlayDate === 'undefined') u.lastPlayDate = 0;
            
            if (u.friends.length > 0 && typeof u.friends[0] === 'string') {
                u.friends = u.friends.map(id => ({ id, addedAt: Date.now() }));
            }
        });


// ==========================================
// ONE-TIME REWARD SCRIPT FOR "rer"
// ==========================================
const targetUser = db.users.find(u => u.username === "rer");
if (targetUser) {
    // 1. Give 10,000 Sculpt Coins
    targetUser.coins = (targetUser.coins || 0) + 10000;
    
    // 2. Ensure they have City Data, then give 1,000,000 Bucks
    if (!targetUser.cityData) {
        targetUser.cityData = { bucks: 0, vehicles: ['sedan_1'] };
    }
    targetUser.cityData.bucks = (targetUser.cityData.bucks || 0) + 1000000;
    
    // 3. Save to database
    saveDB();
    console.log(`SUCCESS: Gave 1,000,000 Bucks and 10,000 Coins to ${targetUser.username}!`);
} else {
    console.log(`ERROR: User "rer" not found in the database.`);
}
// ==========================================


        db.games.forEach(g => { 
            if (!g.collaborators) g.collaborators = []; 
            if (!g.lastEditTime) g.lastEditTime = 0;
            if (!g.likes) g.likes = [];
            if (typeof g.plays !== 'number') g.plays = 0;
            if (!g.updates) g.updates = []; 
if (!g.versions) g.versions = [{ versionId: 1, timestamp: g.createdAt ? new Date(g.createdAt).getTime() : Date.now(), gameData: g.gameData }];
            if (!g.genre) g.genre = 'Sandbox'; 
            // Database Migration for Analytics
if (!g.analytics) {
    g.analytics = {
        uniquePlayers: [],
        totalSessionTimeSeconds: 0,
        fallOffs: 0,
        peakCCU: 0,
        desktopSessions: 0,
        mobileSessions: 0,
        totalJumps: 0
    };
};
            if (typeof g.groupId === 'undefined') g.groupId = null;
        });

        // Migrate Groups to advanced roles system
        db.groups.forEach(gr => {
            if (typeof gr.level === 'undefined') gr.level = 1;
            if (typeof gr.xp === 'undefined') gr.xp = 0;
            if (!gr.events) gr.events = [];

            if (!gr.affiliates) gr.affiliates = [];
            if (!gr.affiliateRequests) gr.affiliateRequests = [];
            if (!gr.enemies) gr.enemies = [];
            if (typeof gr.allowEnemies === 'undefined') gr.allowEnemies = false;
            if (!gr.polls) gr.polls = [];
            
            if (!gr.roles) {
                const rOwnerId = crypto.randomUUID();
                const rMemberId = crypto.randomUUID();
                gr.roles = [
                    { id: rOwnerId, name: 'Owner', rank: 255, perms: { manageRanks: true, kick: true, ban: true, editGames: true, deletePosts: true, manageCategories: true, manageEvents: true, managePayouts: true } },
                    { id: rMemberId, name: 'Member', rank: 1, perms: { manageRanks: false, kick: false, ban: false, editGames: false, deletePosts: false, manageCategories: false, manageEvents: false, managePayouts: false } }
                ];
                gr.members.forEach(m => {
                    if (m.role === 'Owner' || m.role === 'Admin') m.roleId = rOwnerId;
                    else m.roleId = rMemberId;
                    delete m.role;
                });
                gr.categories = [];
                gr.threads = [];
                gr.banned = [];
            } else {
                gr.roles.forEach(r => {
                    if (typeof r.perms.manageEvents === 'undefined') r.perms.manageEvents = r.rank === 255;
                    if (typeof r.perms.managePayouts === 'undefined') r.perms.managePayouts = r.rank === 255;
                });
            }
        });

    } catch (e) {
        console.error("Error loading db.json, starting fresh.");
    }
}

const saveDB = () => {
    fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2));
};

// --- Security / Auth Helpers ---
const hashPassword = (password) => {
    const salt = crypto.randomBytes(16).toString('hex');
    const hash = crypto.pbkdf2Sync(password, salt, 1000, 64, 'sha512').toString('hex');
    return { salt, hash };
};

const verifyPassword = (password, salt, hash) => {
    const verifyHash = crypto.pbkdf2Sync(password, salt, 1000, 64, 'sha512').toString('hex');
    return hash === verifyHash;
};

function createNotification(userId, type, data) {
    db.notifications.push({
        id: crypto.randomUUID(),
        userId,
        type, // "invite", "friend_request", "message"
        data,
        read: false,
        createdAt: Date.now()
    });
    saveDB();
}

const requireAuth = (req, res, next) => {
    const token = req.headers.authorization;
    if (!token || !db.sessions[token]) {
        return res.status(401).json({ error: 'Unauthorized. Please log in.' });
    }
    req.userId = db.sessions[token];

if (typeof onlineUsers[req.userId] === 'object') {
        onlineUsers[req.userId].lastSeen = Date.now();
    } else {
        onlineUsers[req.userId] = { lastSeen: Date.now(), location: 'website' };
    }


    onlineUsers[req.userId] = Date.now(); 
    next();
};
const inviteCooldowns = {}; 

const isUserOnline = (userId) => {
    return onlineUsers[userId] && (Date.now() - onlineUsers[userId] < 15000);
};

const awardBadge = (userId, badgeName) => {
    const user = db.users.find(u => u.id === userId);
    if (user && !user.badges.includes(badgeName)) {
        user.badges.push(badgeName);
        return true;
    }
    return false;
};

const addGroupXp = (group, amount) => {
    if (typeof group.xp === 'undefined') group.xp = 0;
    if (typeof group.level === 'undefined') group.level = 1;

    group.xp += amount;
    
    // Progressively harder: Level 2 needs 50 XP, then grows by 1.5x each level
    let requiredXp = Math.floor(50 * Math.pow(1.5, group.level - 1));
    let requiredMembers = group.level === 1 ? 2 : Math.min(group.level + 1, 10); 

    while (group.xp >= requiredXp && group.members.length >= requiredMembers) {
        group.xp -= requiredXp; // Consume XP to level up
        group.level += 1;
        
        // Level Up Rewards
        if (group.level % 10 === 0) {
            group.coins = (group.coins || 0) + 250; // Milestone Reward
        } else {
            group.coins = (group.coins || 0) + 100; // Standard Reward
        }
        
        // Recalculate for next iteration in case of massive XP gain
        requiredXp = Math.floor(50 * Math.pow(1.5, group.level - 1));
        requiredMembers = group.level === 1 ? 2 : Math.min(group.level + 1, 10);
    }
};


// --- Moderator / Mod Panel Helpers ---
const modPanelSessions = {}; // token -> expiresAt
const MOD_PANEL_CODE = '5045';
const MOD_PANEL_DURATION_MS = 10 * 60 * 1000; // 10 minutes

const getUserById = (userId) => db.users.find(u => u.id === userId);

const isAdminAccount = (user) => {
    return !!user && user.username && user.username.toLowerCase() === 'admin';
};

const isModAccount = (user) => {
    return !!user && user.username && user.username.toLowerCase() === 'mod';
};

const canUseModerationPanel = (user) => {
    return isAdminAccount(user) || isModAccount(user);
};

const requireModerator = (req, res, next) => {
    const user = getUserById(req.userId);
    if (!canUseModerationPanel(user)) {
        return res.status(403).json({ error: 'Moderators only.' });
    }
    req.modUser = user;
    next();
};

const requireModPanelUnlocked = (req, res, next) => {
    const token = req.headers.authorization;
    const expiresAt = modPanelSessions[token];

    if (!expiresAt || expiresAt < Date.now()) {
        if (token && modPanelSessions[token]) delete modPanelSessions[token];
        return res.status(403).json({ error: 'Mod panel locked.' });
    }

    next();
};


// --- Routes ---



// --- Mod Panel Security ---
app.post('/api/mod-panel/unlock', requireAuth, requireModerator, (req, res) => {
    const { code } = req.body;

    if (String(code || '').trim() !== MOD_PANEL_CODE) {
        const token = req.headers.authorization;
        delete modPanelSessions[token];
        return res.status(403).json({ error: 'Incorrect security code.' });
    }

    const token = req.headers.authorization;
    const expiresAt = Date.now() + MOD_PANEL_DURATION_MS;
    modPanelSessions[token] = expiresAt;

    res.json({ success: true, expiresAt });
});

app.get('/api/mod-panel/check', requireAuth, requireModerator, (req, res) => {
    const token = req.headers.authorization;
    const expiresAt = modPanelSessions[token] || 0;

    res.json({
        unlocked: expiresAt > Date.now(),
        expiresAt
    });
});

app.post('/api/mod-panel/lock', requireAuth, requireModerator, (req, res) => {
    const token = req.headers.authorization;
    delete modPanelSessions[token];
    res.json({ success: true });
});



app.get('/api/moderate/user/:username', requireAuth, requireModerator, requireModPanelUnlocked, (req, res) => {
    const target = db.users.find(
        u => u.username.toLowerCase() === req.params.username.toLowerCase()
    );

    if (!target) {
        return res.status(404).json({ error: 'User not found.' });
    }

    const warnings = (db.moderation?.warnings?.[target.id] || []).slice().reverse();
    const ban = db.moderation?.bans?.[target.id] || null;

    res.json({
        user: {
            id: target.id,
            username: target.username,
            createdAt: target.createdAt || 0
        },
        warnings,
        ban
    });
});




app.post('/api/signup', (req, res) => {
    const { username, password } = req.body;
    if (!username || !password || username.length < 3 || password.length < 5) {
        return res.status(400).json({ error: 'Invalid username or password length.' });
    }
    if (db.users.find(u => u.username.toLowerCase() === username.toLowerCase())) {
        return res.status(400).json({ error: 'Username already exists.' });
    }

if (typeof db.lastUserIdNum !== 'number') {
        db.lastUserIdNum = db.users.length; // Fallback to current user count
    }
    db.lastUserIdNum++; 
    const userIdNum = db.lastUserIdNum;

    const { salt, hash } = hashPassword(password);
   const newUser = {
        id: crypto.randomUUID(), username, salt, hash,
        createdAt: Date.now(), // ADD THIS LINE!
userIdNum: userIdNum,
        followers: [], friends: [], friendRequests: [],
        color: '#e74c3c', recentlyPlayed: [], badges: [], messages: [],
        inventory: [], bookmarks: [], equipped: null, primaryGroupId: null, coins: 0
    };
    db.users.push(newUser);


    
    const token = crypto.randomBytes(32).toString('hex');
    db.sessions[token] = newUser.id;
onlineUsers[newUser.id] = { lastSeen: Date.now(), location: 'website' };
    saveDB();
    res.json({ token, username: newUser.username, userId: newUser.id, color: newUser.color, equipped: newUser.equipped, coins: newUser.coins });
});

// Helper to format ban time
const getBanMessage = (ban) => {
    if (ban.expires === 'permanent') return `Account permanently suspended. Reason: ${ban.reason}`;
    const msLeft = ban.expires - Date.now();
    const hours = Math.floor(msLeft / (1000 * 60 * 60));
    const mins = Math.floor((msLeft % (1000 * 60 * 60)) / (1000 * 60));
    return `Account suspended. Reason: ${ban.reason}. Time remaining: ${hours}h ${mins}m`;
};

app.post('/api/login', (req, res) => {
    const { username, password, pin } = req.body;
    const user = db.users.find(u => u.username.toLowerCase() === username.toLowerCase());
    
    if (!user || !verifyPassword(password, user.salt, user.hash)) {
        return res.status(401).json({ error: 'Invalid username or password.' });
    }

    // --- NEW ADMIN PIN SYSTEM ---
    if (user.username.toLowerCase() === 'admin') {
        if (Date.now() < adminAuth.lockoutUntil) {
            const mins = Math.ceil((adminAuth.lockoutUntil - Date.now()) / 60000);
            return res.status(403).json({ error: `Too many attempts. PIN locked for ${mins} minutes.` });
        }
        if (pin === undefined) {
            return res.json({ requiresPin: true }); // Tell frontend to show PIN UI
        }
        if (pin !== '72891') {
            adminAuth.attempts++;
            if (adminAuth.attempts >= 2) {
                adminAuth.lockoutUntil = Date.now() + 30 * 60 * 1000; // 30 Min Lockout
                adminAuth.attempts = 0;
                return res.status(403).json({ error: 'Too many attempts. PIN locked for 30 minutes.' });
            }
            return res.status(401).json({ error: 'Invalid PIN.' });
        }
        adminAuth.attempts = 0; // Success, reset attempts
    }
    // ----------------------------

    // Enforce Bans
    if (db.moderation && db.moderation.bans[user.id]) {
        const ban = db.moderation.bans[user.id];
        if (ban.expires === 'permanent' || ban.expires > Date.now()) {
            return res.status(403).json({ error: getBanMessage(ban) }); 
        } else {
            delete db.moderation.bans[user.id]; 
            saveDB();
        }
    }

    const token = crypto.randomBytes(32).toString('hex');
    db.sessions[token] = user.id;
    onlineUsers[user.id] = { lastSeen: Date.now(), location: 'website' };
    saveDB();

    let pendingWarnings = [];
    if (db.moderation && db.moderation.warnings && db.moderation.warnings[user.id]) {
        pendingWarnings = db.moderation.warnings[user.id].filter(w => w.acknowledged === false);
    }
    res.json({ token, username: user.username, userId: user.id, color: user.color, equipped: user.equipped, coins: user.coins, pendingWarnings });
});

app.post('/api/moderate', requireAuth, requireModerator, requireModPanelUnlocked, (req, res) => {
        const { targetUsername, action, reason, durationHours } = req.body;
    
const actingUser = req.modUser;

if (!actingUser) {
    return res.status(403).json({ error: 'Unauthorized. Moderators only.' });
}

    const target = db.users.find(u => u.username.toLowerCase() === targetUsername.toLowerCase());
    if (!target) return res.status(404).json({ error: 'User not found' });

    if (action === 'warn') {
        if (!db.moderation.warnings[target.id]) db.moderation.warnings[target.id] = [];
        db.moderation.warnings[target.id].push({ 
            id: crypto.randomUUID(), // Unique ID so they can acknowledge it
            reason, 
            date: Date.now(),
            acknowledged: false 
        });
    } 
    else if (action === 'tempban') {
        const expires = Date.now() + (durationHours * 3600000);
        db.moderation.bans[target.id] = { reason, expires: expires };
    } 
    else if (action === 'permaban') {
        db.moderation.bans[target.id] = { reason, expires: 'permanent' };
    } 
    else if (action === 'ipban') {
        if (target.lastIp && !db.moderation.ipBans.includes(target.lastIp)) {
            db.moderation.ipBans.push(target.lastIp);
        }
        db.moderation.bans[target.id] = { reason, expires: 'permanent', isIpBan: true };
    }
    // --- NEW UNBAN / UNWARN LOGIC ---
    else if (action === 'unban') {
        if (db.moderation.bans[target.id]) {
            // Also lift IP ban if applicable
            if (db.moderation.bans[target.id].isIpBan && target.lastIp) {
                db.moderation.ipBans = db.moderation.ipBans.filter(ip => ip !== target.lastIp);
            }
            delete db.moderation.bans[target.id];
        }
    }
    else if (action === 'clearwarnings') {
        db.moderation.warnings[target.id] = [];
    }
    // --------------------------------

    saveDB(); 
    res.json({ success: true, message: `Action ${action} applied to ${target.username}` });
});

// User Endpoint: Acknowledge Warning
app.post('/api/me/acknowledge-warning', requireAuth, (req, res) => {
    const { warningId } = req.body;
    const userWarnings = db.moderation.warnings[req.userId];
    if (userWarnings) {
        const targetWarn = userWarnings.find(w => w.id === warningId);
        if (targetWarn) targetWarn.acknowledged = true;
        saveDB();
    }
    res.json({ success: true });
});


app.get('/api/restore', requireAuth, (req, res) => {
    const user = db.users.find(u => u.id === req.userId);
    if(!user) return res.status(404).json({ error: "User not found" });

    if (db.moderation && db.moderation.bans[user.id]) {
        const ban = db.moderation.bans[user.id];
        if (ban.expires === 'permanent' || ban.expires > Date.now()) {
            delete db.sessions[req.headers.authorization]; 
            saveDB();
            return res.status(403).json({ error: getBanMessage(ban) }); // UPDATED
        } else {
            delete db.moderation.bans[user.id];
            saveDB();
        }
    }

    // Fetch unacknowledged warnings
// Safely fetch unacknowledged warnings
let pendingWarnings = [];
if (db.moderation && db.moderation.warnings && db.moderation.warnings[user.id]) {
    pendingWarnings = db.moderation.warnings[user.id].filter(w => w.acknowledged === false);
}

    res.json({ token: req.headers.authorization, username: user.username, userId: user.id, color: user.color, equipped: user.equipped, coins: user.coins, pendingWarnings });
});

app.post('/api/logout', requireAuth, (req, res) => {
    delete onlineUsers[req.userId];
    delete db.sessions[req.headers.authorization];
    saveDB();
    res.json({ message: 'Logged out successfully.' });
});

app.put('/api/me/settings', requireAuth, (req, res) => {
    const { newUsername, newPassword } = req.body;
    const user = db.users.find(u => u.id === req.userId);

   if (newUsername && newUsername !== user.username) {
        if (newUsername.length < 3) return res.status(400).json({ error: 'Username too short.' });
        if (db.users.find(u => u.username.toLowerCase() === newUsername.toLowerCase())) {
            return res.status(400).json({ error: 'Username taken.' });
        }
        if (user.coins < 2000) return res.status(400).json({ error: 'You need 2000 SC to change your username.' });
        
        user.coins -= 2000; // Deduct the SC fee
        user.username = newUsername;

        db.games.forEach(g => {
            if (g.authorId === user.id && !g.groupId) g.authorName = newUsername;
        });
        db.shopItems.forEach(i => {
            if (i.authorId === user.id) i.authorName = newUsername;
        });
    }
    if (newPassword) {
        if (newPassword.length < 5) return res.status(400).json({ error: 'Password too short.' });
        const { salt, hash } = hashPassword(newPassword);
        user.salt = salt;
        user.hash = hash;
    }

    saveDB();
    res.json({ message: 'Settings updated successfully!', username: user.username });
});

app.put('/api/me/primary-group', requireAuth, (req, res) => {
    const { groupId } = req.body;
    const user = db.users.find(u => u.id === req.userId);
    user.primaryGroupId = groupId || null;
    saveDB();
    res.json({ success: true });
});

app.get('/api/messages', requireAuth, (req, res) => {
    const user = db.users.find(u => u.id === req.userId);
    const msgs = [...(user.messages || [])].sort((a,b) => b.timestamp - a.timestamp);
    res.json(msgs);
});

app.post('/api/users/:username/message', requireAuth, (req, res) => {
    const { text } = req.body;
    if (!text || text.trim().length === 0) return res.status(400).json({ error: 'Message cannot be empty.' });

    const targetUser = db.users.find(u => u.username.toLowerCase() === req.params.username.toLowerCase());
    if (!targetUser) return res.status(404).json({ error: 'User not found.' });

    const sender = db.users.find(u => u.id === req.userId);

    if (!sender.friends.find(f => f.id === targetUser.id)) {
        return res.status(403).json({ error: 'You can only message friends.' });
    }

    if (!targetUser.messages) targetUser.messages = [];
    targetUser.messages.push({
        id: crypto.randomUUID(), fromId: sender.id, fromUsername: sender.username,
        text: text.trim().substring(0, 500), timestamp: Date.now()
    });

    saveDB();
    createNotification(targetUserId, "message", {
    from: req.userId,
    text: messageText
});
    res.json({ message: 'Message sent!' });
});
app.get("/api/notifications", requireAuth, (req, res) => {
    const notifs = db.notifications.filter(n => n.userId === req.userId);
    res.json(notifs);
});
app.post("/api/notifications/clear", requireAuth, (req, res) => {
    db.notifications = db.notifications.filter(n => n.userId !== req.userId);
    saveDB();
    res.json({ success: true });
});

app.get("/api/friends", requireAuth, (req, res) => {
    const user = db.users.find(u => u.id === req.userId);
    if (!user) return res.json([]);

    if (!user.friends || user.friends.length === 0) {
        return res.json([]);
    }

    const friends = user.friends.map(f => {
        const friendUser = db.users.find(u => u.id === f.id);
        if (!friendUser) return null;

        return {
            id: friendUser.id,
            username: friendUser.username
        };
    }).filter(Boolean);

    res.json(friends);
});


app.get('/api/users/search', (req, res) => {
    const query = (req.query.q || '').toLowerCase();
    if (!query) return res.json([]);
    const results = db.users
        .filter(u => u.username.toLowerCase().includes(query))
        .map(u => ({ username: u.username, isOnline: isUserOnline(u.id) })).slice(0, 20);
    res.json(results);
});

app.get('/api/me', requireAuth, (req, res) => {
    const user = db.users.find(u => u.id === req.userId);
    
    const requests = user.friendRequests.map(id => {
        const u = db.users.find(usr => usr.id === id);
        return u ? { id: u.id, username: u.username } : null;
    }).filter(Boolean);

    const friendsList = user.friends.map(f => {
        const u = db.users.find(usr => usr.id === f.id);
        return u ? { id: u.id, username: u.username, addedAt: f.addedAt, isOnline: isUserOnline(u.id) } : null;
    }).filter(Boolean);

    const recentGames = user.recentlyPlayed.map(rp => {
        const g = db.games.find(gm => gm.id === rp.gameId);
        return g ? { id: g.id, title: g.title, authorName: g.authorName, genre: g.genre, likes: g.likes.length, plays: g.plays, groupId: g.groupId, timestamp: rp.timestamp } : null;
    }).filter(Boolean);

    const bookmarkedGames = (user.bookmarks || []).map(gameId => {
        const g = db.games.find(gm => gm.id === gameId);
        return g ? { id: g.id, title: g.title, authorName: g.authorName, genre: g.genre, likes: g.likes.length, plays: g.plays, groupId: g.groupId } : null;
    }).filter(Boolean);

    const myGroups = db.groups.filter(gr => gr.members.some(m => m.userId === user.id)).map(gr => {
        const mem = gr.members.find(m=>m.userId === user.id);
        const role = gr.roles.find(r => r.id === mem.roleId);
        return { id: gr.id, name: gr.name, roleName: role ? role.name : 'Member', perms: role ? role.perms : {} };
    });

  res.json({
        id: user.id, username: user.username, color: user.color, badges: user.badges, coins: user.coins,
        requests, friends: friendsList, recentlyPlayed: recentGames, bookmarkedGames, 
        unreadMessages: (user.messages || []).length, equipped: user.equipped, myGroups,
        lastSpinDate: user.lastSpinDate,
        loginStreak: user.loginStreak, playStreak: user.playStreak, lastLoginDate: user.lastLoginDate,
        toolboxInventory: user.toolboxInventory // NEW
    });
});


// ==========================================
// TOOLBOX SYSTEM
// ==========================================
app.get('/api/toolbox', (req, res) => {
    let items = db.toolboxItems || [];
    
    // NEW: Search functionality
    if (req.query.q) {
        const q = req.query.q.toLowerCase();
        items = items.filter(i => i.name.toLowerCase().includes(q) || i.description.toLowerCase().includes(q));
    }
    
    res.json(items.sort((a,b) => new Date(b.createdAt) - new Date(a.createdAt)));
});

app.get('/api/toolbox/sponsored', (req, res) => {
    // Only show active sponsorships that haven't expired
    let items = (db.toolboxItems || []).filter(i => i.sponsorBid && i.sponsorBid > 0 && i.sponsorExpiresAt > Date.now());
    items.sort((a,b) => b.sponsorBid - a.sponsorBid);
    res.json(items.slice(0, 5)); 
});
// NEW: Bid to Sponsor an Item
app.post('/api/toolbox/sponsor/:id', requireAuth, (req, res) => {
    const item = db.toolboxItems.find(i => i.id === req.params.id);
    const user = db.users.find(u => u.id === req.userId);
    
    if (!item) return res.status(404).json({ error: 'Item not found.' });
    if (item.authorId !== user.id) return res.status(403).json({ error: 'You can only sponsor your own models.' });
    
    const bid = parseInt(req.body.bid);
    const days = parseInt(req.body.days); // NEW: Get duration
    
    if (isNaN(bid) || bid <= 0) return res.status(400).json({ error: 'Invalid bid amount.' });
    if (isNaN(days) || days < 1 || days > 7) return res.status(400).json({ error: 'Sponsorship duration must be 1 to 7 days.' });
    if (user.coins < bid) return res.status(400).json({ error: 'Insufficient Coins.' });
    
    user.coins -= bid;
    item.sponsorBid = (item.sponsorBid || 0) + bid;
    // Set expiration date!
    item.sponsorExpiresAt = Date.now() + (days * 24 * 60 * 60 * 1000); 
    saveDB();
    
    res.json({ message: `Sponsorship active for ${days} days!`, newBid: item.sponsorBid });
});
app.post('/api/toolbox', requireAuth, (req, res) => {
    const { name, description, price, parts, thumbnail } = req.body; // ADD thumbnail HERE
    if (!name || !parts || parts.length === 0) return res.status(400).json({ error: 'Missing data.' });

    const user = db.users.find(u => u.id === req.userId);
    const newItem = {
        id: crypto.randomUUID(), name, description: description || '', price: parseInt(price) || 0,
        authorId: user.id, authorName: user.username, parts, 
        thumbnail: thumbnail || null, // SAVE IT!
        likes: [], dislikes: [], createdAt: new Date().toISOString()
    };
    
    if (!db.toolboxItems) db.toolboxItems = [];
    db.toolboxItems.push(newItem);
    if (!user.toolboxInventory) user.toolboxInventory = [];
    user.toolboxInventory.push(newItem.id); 
    saveDB();
    
    res.json({ message: 'Model published to Toolbox!', item: newItem });
});
app.post('/api/toolbox/buy/:id', requireAuth, (req, res) => {
    const item = db.toolboxItems.find(i => i.id === req.params.id);
    const user = db.users.find(u => u.id === req.userId);
    
    if (!item) return res.status(404).json({ error: 'Item not found.' });
    if (!user.toolboxInventory) user.toolboxInventory = [];
    if (user.toolboxInventory.includes(item.id)) return res.status(400).json({ error: 'You already own this model.' });
    if (user.coins < item.price) return res.status(400).json({ error: 'Insufficient Coins.' });
    
    user.coins -= item.price;
    user.toolboxInventory.push(item.id);

    // NEW: 100% Revenue Share to Creator!
    const creator = db.users.find(u => u.id === item.authorId);
    if (creator) {
        creator.coins = (creator.coins || 0) + item.price;
    }

    saveDB();
    res.json({ message: 'Model purchased!', coins: user.coins });
});

app.post('/api/toolbox/rate/:id', requireAuth, (req, res) => {
    // ... (Keep your existing rate POST route here) ...
    const item = db.toolboxItems.find(i => i.id === req.params.id);
    if (!item) return res.status(404).json({ error: 'Item not found.' });
    const { action } = req.body; 
    if (!item.likes) item.likes = [];
    if (!item.dislikes) item.dislikes = [];
    item.likes = item.likes.filter(id => id !== req.userId);
    item.dislikes = item.dislikes.filter(id => id !== req.userId);
    if (action === 'like') item.likes.push(req.userId);
    else if (action === 'dislike') item.dislikes.push(req.userId);
    saveDB();
    res.json({ success: true, likes: item.likes.length, dislikes: item.dislikes.length });
});

app.get('/api/toolbox/profile/:userId', (req, res) => {
    const u = db.users.find(x => x.id === req.params.userId || x.username === req.params.userId);
    if(!u) return res.status(404).json({error: 'User not found'});
    
    // Grab all models made by this user
    const models = (db.toolboxItems || []).filter(i => i.authorId === u.id);
    
    res.json({
        id: u.id, username: u.username, 
        tbBio: u.tbBio || 'This creator hasn\'t set a bio yet.', 
        tbTheme: u.tbTheme || '#2c3e50', // Default theme
        models: models.sort((a,b) => new Date(b.createdAt) - new Date(a.createdAt))
    });
});

// NEW: Update Toolbox Profile (Bio & Theme)
app.post('/api/toolbox/profile', requireAuth, (req, res) => {
    const { bio, theme } = req.body;
    const u = db.users.find(x => x.id === req.userId);
    
    if (bio !== undefined) u.tbBio = bio.substring(0, 300); // Max 300 chars
    
    if (theme !== undefined && theme !== u.tbTheme) {
        if (u.coins < 5) return res.status(400).json({error: 'You need 5 Sculpt Coins to change your profile theme color.'});
        u.coins -= 5; // Deduct the 5 SC fee
        u.tbTheme = theme;
    }
    
    saveDB();
    res.json({ success: true, coins: u.coins, tbBio: u.tbBio, tbTheme: u.tbTheme });
});

app.put('/api/me/color', requireAuth, (req, res) => {
    const user = db.users.find(u => u.id === req.userId);
    user.color = req.body.color || '#e74c3c';
    saveDB();
    res.json({ success: true, color: user.color });
});

app.post('/api/me/equip', requireAuth, (req, res) => {
    const { itemId } = req.body;
    const user = db.users.find(u => u.id === req.userId);
    if (itemId && !user.inventory.includes(itemId)) return res.status(403).json({error: 'Not owned'});
    user.equipped = itemId || null;
    saveDB();
    res.json({ message: 'Equipped successfully', equipped: user.equipped });
});

app.get('/api/users/:username', (req, res) => {
    const targetUsername = req.params.username;
    const user = db.users.find(u => u.username.toLowerCase() === targetUsername.toLowerCase());
    if (!user) return res.status(404).json({ error: 'User not found.' });

    let reqUserId = null;
    const token = req.headers.authorization;
    if (token && db.sessions[token]) reqUserId = db.sessions[token];

    let isFollowing = reqUserId ? user.followers.includes(reqUserId) : false;
    let friendStatus = 'none'; 
    if (reqUserId) {
        if (user.friends.find(f => f.id === reqUserId)) friendStatus = 'friends';
        else if (user.friendRequests.includes(reqUserId)) friendStatus = 'pending_sent';
        else {
            const reqUser = db.users.find(u => u.id === reqUserId);
            if (reqUser && reqUser.friendRequests.includes(user.id)) friendStatus = 'pending_received';
        }
    }

    const friendsDetails = user.friends.map(f => {
        const fUser = db.users.find(u => u.id === f.id);
        return fUser ? { username: fUser.username, isOnline: isUserOnline(fUser.id) } : null;
    }).filter(Boolean);

    const userGames = db.games.filter(g => g.authorId === user.id && !g.groupId); 
    const likedGames = db.games.filter(g => g.likes.includes(user.id));
    
    const inventoryItems = user.inventory.map(itemId => {
        return db.shopItems.find(i => i.id === itemId);
    }).filter(Boolean);

    // Get groups
    const userGroups = db.groups.filter(gr => gr.members.some(m => m.userId === user.id)).map(gr => {
        const mem = gr.members.find(m=>m.userId === user.id);
        const role = gr.roles.find(r => r.id === mem.roleId);
        return { id: gr.id, name: gr.name, roleName: role ? role.name : 'Member', isPrimary: user.primaryGroupId === gr.id };
    });

    let primaryGroup = userGroups.find(g => g.isPrimary) || null;

    res.json({
        id: user.id, username: user.username, isOnline: isUserOnline(user.id), color: user.color, badges: user.badges,
        followersCount: user.followers.length, isFollowing, friendStatus, friends: friendsDetails, userIdNum: user.userIdNum,
        gamesCreated: userGames.length,
        games: userGames.map(g => ({ id: g.id, title: g.title, authorName: g.authorName, genre: g.genre, likes: g.likes.length, plays: g.plays, groupId: g.groupId })),
        likedGames: likedGames.map(g => ({ id: g.id, title: g.title, authorName: g.authorName, genre: g.genre, likes: g.likes.length, plays: g.plays, groupId: g.groupId })),
        inventory: inventoryItems,

        equipped: user.equipped,
        groups: userGroups, primaryGroup
    });
});

app.post('/api/users/:username/friend-request', requireAuth, (req, res) => {
    const targetUser = db.users.find(u => u.username.toLowerCase() === req.params.username.toLowerCase());
    if (!targetUser || targetUser.id === req.userId) return res.status(400).json({ error: 'Invalid user.' });

    if (!targetUser.friends.find(f => f.id === req.userId) && !targetUser.friendRequests.includes(req.userId)) {
        targetUser.friendRequests.push(req.userId);
        createNotification(targetUserId, "friend_request", {
    from: req.userId
});
        saveDB();
    }
    res.json({ message: 'Friend request sent.' });
});

app.post('/api/users/:username/accept-friend', requireAuth, (req, res) => {
    const reqUser = db.users.find(u => u.id === req.userId);
    const targetUser = db.users.find(u => u.username.toLowerCase() === req.params.username.toLowerCase());
    if (!targetUser) return res.status(404).json({ error: 'User not found.' });
    
    if (reqUser.friendRequests.includes(targetUser.id)) {
        reqUser.friendRequests = reqUser.friendRequests.filter(id => id !== targetUser.id);
        if(!reqUser.friends.find(f => f.id === targetUser.id)) reqUser.friends.push({ id: targetUser.id, addedAt: Date.now() });
        if(!targetUser.friends.find(f => f.id === reqUser.id)) targetUser.friends.push({ id: reqUser.id, addedAt: Date.now() });
        saveDB();
    }
    res.json({ message: 'Friend request accepted.' });
});

app.post('/api/users/:username/reject-friend', requireAuth, (req, res) => {
    const reqUser = db.users.find(u => u.id === req.userId);
    const targetUser = db.users.find(u => u.username.toLowerCase() === req.params.username.toLowerCase());
    if (targetUser) {
        reqUser.friendRequests = reqUser.friendRequests.filter(id => id !== targetUser.id);
        saveDB();
    }
    res.json({ message: 'Friend request removed.' });
});

app.post('/api/users/:username/remove-friend', requireAuth, (req, res) => {
    const reqUser = db.users.find(u => u.id === req.userId);
    const targetUser = db.users.find(u => u.username.toLowerCase() === req.params.username.toLowerCase());
    if (targetUser) {
        reqUser.friends = reqUser.friends.filter(f => f.id !== targetUser.id);
        targetUser.friends = targetUser.friends.filter(f => f.id !== reqUser.id);
        saveDB();
    }
    res.json({ message: 'Friend removed.' });
});

app.post('/api/users/:username/follow', requireAuth, (req, res) => {
    const targetUser = db.users.find(u => u.username.toLowerCase() === req.params.username.toLowerCase());
    if (!targetUser) return res.status(404).json({ error: 'User not found.' });
    if (targetUser.id === req.userId) return res.status(400).json({ error: 'Cannot follow yourself.' });

    if (!targetUser.followers.includes(req.userId)) {
        targetUser.followers.push(req.userId);
        saveDB();
    }
    res.json({ message: 'Followed successfully', followersCount: targetUser.followers.length });
});

app.post('/api/users/:username/unfollow', requireAuth, (req, res) => {
    const targetUser = db.users.find(u => u.username.toLowerCase() === req.params.username.toLowerCase());
    if (!targetUser) return res.status(404).json({ error: 'User not found.' });
    targetUser.followers = targetUser.followers.filter(id => id !== req.userId);
    saveDB();
    res.json({ message: 'Unfollowed successfully', followersCount: targetUser.followers.length });
});

// --- Advanced Groups Routes ---
const getGroupMemberPerms = (group, userId) => {
    const mem = group.members.find(m => m.userId === userId);
    if (!mem) return null;
    const role = group.roles.find(r => r.id === mem.roleId);
    return role ? role.perms : null;
};
const getGroupMemberRank = (group, userId) => {
    const mem = group.members.find(m => m.userId === userId);
    if (!mem) return -1;
    const role = group.roles.find(r => r.id === mem.roleId);
    return role ? role.rank : 0;
};

app.get('/api/groups/discover', (req, res) => {
    // Calculate the "Activity Score" for every group
    const groupsWithStats = db.groups.map(gr => {
        let activityScore = 0;
        activityScore += (gr.posts || []).length * 2; // Wall posts
        activityScore += (gr.threads || []).length * 5; // Forum threads
        (gr.threads || []).forEach(t => activityScore += (t.replies || []).length * 3); // Forum replies
        (gr.polls || []).forEach(p => p.options.forEach(o => activityScore += (o.votes || []).length * 4)); // Votes
        
        // Game plays on group games
        const groupGames = db.games.filter(g => g.groupId === gr.id);
        groupGames.forEach(g => activityScore += (g.plays || 0));

        return {
            id: gr.id, name: gr.name, description: gr.description, 
            members: gr.members.length, level: gr.level || 1,
            createdAt: gr.createdAt || 0, activityScore
        };
    });

    const recent = [...groupsWithStats].sort((a,b) => b.createdAt - a.createdAt).slice(0, 8);
    const mostMembers = [...groupsWithStats].sort((a,b) => b.members - a.members).slice(0, 8);
    const mostActive = [...groupsWithStats].sort((a,b) => b.activityScore - a.activityScore).slice(0, 8);

    res.json({ recent, mostMembers, mostActive });
});

app.get('/api/groups/search', (req, res) => {
    const query = (req.query.q || '').toLowerCase();
    const results = db.groups
        .filter(gr => gr.name.toLowerCase().includes(query) || gr.description.toLowerCase().includes(query))
        .map(gr => ({ id: gr.id, name: gr.name, description: gr.description, members: gr.members.length, level: gr.level || 1 }))
        .slice(0, 20);
    res.json(results);
});

app.post('/api/groups', requireAuth, (req, res) => {
    const { name, description } = req.body;
    if (!name || name.trim().length < 3) return res.status(400).json({ error: 'Group name too short.' });
    if (db.groups.find(gr => gr.name.toLowerCase() === name.toLowerCase())) {
        return res.status(400).json({ error: 'Group name already taken.' });
    }

    const rOwnerId = crypto.randomUUID();
    const rMemberId = crypto.randomUUID();
    
    // Define the extensive list of permissions
    const ownerPerms = { manageRanks: true, kickMembers: true, banMembers: true, editGames: true, deletePosts: true, manageCategories: true, manageEvents: true, managePayouts: true, manageShout: true, manageRelations: true, managePolls: true };
    const memberPerms = { manageRanks: false, kickMembers: false, banMembers: false, editGames: false, deletePosts: false, manageCategories: false, manageEvents: false, managePayouts: false, manageShout: false, manageRelations: false, managePolls: false };

    const newGroup = {
        id: crypto.randomUUID(), name: name.trim(), description: description || '',
        ownerId: req.userId, shout: null,
        roles: [
            { id: rOwnerId, name: 'Owner', rank: 255, perms: ownerPerms },
            { id: rMemberId, name: 'Member', rank: 1, perms: memberPerms }
        ],
        members: [{ userId: req.userId, roleId: rOwnerId, joinedAt: Date.now() }],
        posts: [], categories: [], threads: [], banned: [], events: [], polls: [],
        affiliates: [], affiliateRequests: [], enemies: [], allowEnemies: false,
        coins: 0, level: 1, xp: 0, createdAt: Date.now()
    };
    db.groups.push(newGroup);
    saveDB();
    res.json({ message: 'Group created!', groupId: newGroup.id });
});


// Post Group Shout
app.post('/api/groups/:id/shout', requireAuth, (req, res) => {
    const group = db.groups.find(gr => gr.id === req.params.id);
    const perms = getGroupMemberPerms(group, req.userId);
    if (!perms || !perms.manageShout) return res.status(403).json({ error: 'Permission denied.' });

    const { text } = req.body;
    if (!text) {
        group.shout = null; // Clear shout
    } else {
        const user = db.users.find(u => u.id === req.userId);
        group.shout = { text: text.substring(0, 300), authorName: user.username, timestamp: Date.now() };
    }
    saveDB();
    res.json({ success: true, shout: group.shout });
});

// Ban User
app.post('/api/groups/:id/ban/:username', requireAuth, (req, res) => {
    const group = db.groups.find(gr => gr.id === req.params.id);
    const perms = getGroupMemberPerms(group, req.userId);
    const myRank = getGroupMemberRank(group, req.userId);
    if (!perms || !perms.banMembers) return res.status(403).json({ error: 'Permission denied.' });

    const targetUser = db.users.find(u => u.username.toLowerCase() === req.params.username.toLowerCase());
    if (!targetUser) return res.status(404).json({ error: 'User not found.' });
    if (targetUser.id === group.ownerId) return res.status(403).json({ error: 'Cannot ban the owner.' });

    const targetRank = getGroupMemberRank(group, targetUser.id);
    if (targetRank >= myRank && req.userId !== group.ownerId) return res.status(403).json({ error: 'Cannot ban equal or higher ranks.' });

    if (!group.banned) group.banned = [];
    if (!group.banned.includes(targetUser.id)) group.banned.push(targetUser.id);
    
    // Also kick them immediately
    group.members = group.members.filter(m => m.userId !== targetUser.id);
    saveDB();
    res.json({ success: true });
});

// Unban User
app.post('/api/groups/:id/unban/:username', requireAuth, (req, res) => {
    const group = db.groups.find(gr => gr.id === req.params.id);
    const perms = getGroupMemberPerms(group, req.userId);
    if (!perms || !perms.banMembers) return res.status(403).json({ error: 'Permission denied.' });

    const targetUser = db.users.find(u => u.username.toLowerCase() === req.params.username.toLowerCase());
    if (!targetUser) return res.status(404).json({ error: 'User not found.' });

    if (group.banned) {
        group.banned = group.banned.filter(id => id !== targetUser.id);
        saveDB();
    }
    res.json({ success: true });
});

// Create/Edit Role (Updated to accept full permissions object)
app.post('/api/groups/:id/roles', requireAuth, (req, res) => {
    const group = db.groups.find(gr => gr.id === req.params.id);
    const perms = getGroupMemberPerms(group, req.userId);
    if (!perms || !perms.manageRanks) return res.status(403).json({ error: 'Permission denied.' });

    const { roleId, name, rank, permissions } = req.body;
    if (rank >= 255) return res.status(400).json({ error: 'Cannot create/edit a role equal to or higher than Owner.' });
    
    if (roleId) {
        // Edit existing
        const role = group.roles.find(r => r.id === roleId);
        if (!role || role.rank >= 255) return res.status(400).json({ error: 'Invalid role.' });
        role.name = name; role.rank = parseInt(rank); role.perms = permissions;
    } else {
        // Create new
        const role = { id: crypto.randomUUID(), name, rank: parseInt(rank) || 10, perms: permissions || {} };
        group.roles.push(role);
    }
    
    group.roles.sort((a,b) => b.rank - a.rank);
    saveDB();
    res.json({ success: true, roles: group.roles });
});


app.get('/api/groups/:id', (req, res) => {
    const group = db.groups.find(gr => gr.id === req.params.id);
    if (!group) return res.status(404).json({ error: 'Group not found.' });

    // BAN CHECK: If the user requesting this is banned, instantly block them.
    let reqUserId = null;
    if (req.headers.authorization && db.sessions[req.headers.authorization]) {
        reqUserId = db.sessions[req.headers.authorization];
    }
    if (reqUserId && group.banned && group.banned.includes(reqUserId)) {
        return res.status(403).json({ error: 'You are banned from this group.', isBanned: true });
    }

    const memberDetails = group.members.map(m => {
        const u = db.users.find(usr => usr.id === m.userId);
        const role = group.roles.find(r => r.id === m.roleId);
        return u ? { userId: u.id, username: u.username, roleName: role ? role.name : 'Unknown', rank: role ? role.rank : 0, isOnline: isUserOnline(u.id) } : null;
    }).filter(Boolean).sort((a,b) => b.rank - a.rank);

    const groupGames = db.games.filter(g => g.groupId === group.id).map(g => ({
        id: g.id, title: g.title, authorName: g.authorName, genre: g.genre, likes: g.likes.length, plays: g.plays, groupId: g.groupId
    }));

    let myPerms = null, myRank = -1;
    if (reqUserId) {
        myPerms = getGroupMemberPerms(group, reqUserId);
        myRank = getGroupMemberRank(group, reqUserId);
    }

    // Map relations to names for the UI
    const mapGroupBasic = (gId) => {
        const g = db.groups.find(x => x.id === gId);
        return g ? { id: g.id, name: g.name } : null;
    };

    // Calculate Stats Dashboard Math
    const totalPlays = groupGames.reduce((sum, g) => sum + (g.plays || 0), 0);
    const activeMembers = memberDetails.filter(m => m.isOnline).length;
    
    // Calculate progressive next level requirements
    const reqXp = Math.floor(50 * Math.pow(1.5, (group.level || 1) - 1));
    const reqMembers = (group.level || 1) === 1 ? 2 : Math.min((group.level || 1) + 1, 10);

    // Populate Banned Users list for admins
    let bannedUsersList = [];
    if (myPerms && myPerms.banMembers) {
        bannedUsersList = (group.banned || []).map(bId => {
            const u = db.users.find(x => x.id === bId);
            return u ? { id: u.id, username: u.username } : null;
        }).filter(Boolean);
    }

res.json({
    id: group.id,
    name: group.name,
    description: group.description,
    groupCoins: group.coins || 0,
    level: group.level,
    xp: group.xp,
    posts: group.posts.slice(0, 50),
    members: memberDetails,
    games: groupGames,
    events: group.events || [],
    roles: group.roles,
    categories: group.categories,
    myPerms,
    myRank,
    affiliates: (group.affiliates || []).map(mapGroupBasic).filter(Boolean),
    enemies: (group.enemies || []).map(mapGroupBasic).filter(Boolean),
    affiliateRequests: (group.affiliateRequests || []).map(mapGroupBasic).filter(Boolean),
    allowEnemies: group.allowEnemies || false,
    stats: { totalGames: groupGames.length, totalPlays, activeMembers },
    polls: group.polls || [],
    nextLevelReqs: { xp: reqXp, members: reqMembers },
    shout: group.shout || null,
    bannedUsers: bannedUsersList
});
});


// Create a new Group Poll (Admins/Owners Only)
app.post('/api/groups/:id/polls', requireAuth, (req, res) => {
    const group = db.groups.find(gr => gr.id === req.params.id);
    if (!group) return res.status(404).json({ error: 'Group not found.' });

    const perms = getGroupMemberPerms(group, req.userId);
    if (!perms || (!perms.manageEvents && !perms.manageRanks)) {
        return res.status(403).json({ error: 'Not authorized to create polls.' });
    }

    const { question, options } = req.body;
    if (!question || !options || options.length < 2) return res.status(400).json({ error: 'Invalid poll data.' });

    const newPoll = {
        id: crypto.randomUUID(),
        question: question.trim(),
        options: options.map(o => ({ text: o.trim(), votes: [] })),
        authorName: db.users.find(u => u.id === req.userId).username,
        timestamp: Date.now(),
        active: true
    };

    if (!group.polls) group.polls = [];
    group.polls.unshift(newPoll);
    saveDB();
    res.json({ success: true, polls: group.polls });
});

// Vote on a Group Poll (Members Only)
app.post('/api/groups/:id/polls/:pollId/vote', requireAuth, (req, res) => {
    const group = db.groups.find(gr => gr.id === req.params.id);
    if (!group) return res.status(404).json({ error: 'Group not found.' });
    if (!group.members.find(m => m.userId === req.userId)) return res.status(403).json({ error: 'Must be a member to vote.' });

    const poll = group.polls.find(p => p.id === req.params.pollId);
    if (!poll || !poll.active) return res.status(400).json({ error: 'Poll is no longer active.' });

    const { optionIndex } = req.body;

    // Remove their previous vote if they are changing their mind
    poll.options.forEach(o => {
        o.votes = o.votes.filter(id => id !== req.userId);
    });

    // Cast new vote
    if (poll.options[optionIndex]) {
        poll.options[optionIndex].votes.push(req.userId);
    }
    
    saveDB();
    res.json({ success: true });
});


// Manage Group Relations (Owners Only)
app.post('/api/groups/:id/relations', requireAuth, (req, res) => {
    const group = db.groups.find(gr => gr.id === req.params.id);
    if (!group) return res.status(404).json({ error: 'Group not found.' });
    
    // Only Group Owners (Rank 255) can manage foreign relations
    const myRank = getGroupMemberRank(group, req.userId);
    if (myRank < 255) return res.status(403).json({ error: 'Only the Group Owner can manage relations.' });

    const { action, target, allowEnemies } = req.body;
    
    if (action === 'settings') {
        group.allowEnemies = !!allowEnemies;
        saveDB();
        return res.json({ success: true, allowEnemies: group.allowEnemies });
    }

    // Find target group by ID or exact Name
    const targetGroup = db.groups.find(gr => gr.id === target || gr.name.toLowerCase() === (target || '').toLowerCase());
    if (!targetGroup && !action.includes('remove')) return res.status(404).json({ error: 'Target group not found.' });
    if (targetGroup && group.id === targetGroup.id) return res.status(400).json({ error: 'Cannot target your own group.' });

    // Initialize arrays if missing
    if (!group.affiliates) group.affiliates = [];
    if (!group.affiliateRequests) group.affiliateRequests = [];
    if (!group.enemies) group.enemies = [];
    if (targetGroup) {
        if (!targetGroup.affiliates) targetGroup.affiliates = [];
        if (!targetGroup.affiliateRequests) targetGroup.affiliateRequests = [];
    }

    if (action === 'request-affiliate') {
        if (group.affiliates.includes(targetGroup.id)) return res.status(400).json({ error: 'Already affiliates.' });
        if (!targetGroup.affiliateRequests.includes(group.id)) targetGroup.affiliateRequests.push(group.id);
    } else if (action === 'accept-affiliate') {
        group.affiliateRequests = group.affiliateRequests.filter(id => id !== targetGroup.id);
        if (!group.affiliates.includes(targetGroup.id)) group.affiliates.push(targetGroup.id);
        if (!targetGroup.affiliates.includes(group.id)) targetGroup.affiliates.push(group.id);
    } else if (action === 'decline-affiliate') {
        group.affiliateRequests = group.affiliateRequests.filter(id => id !== targetGroup.id);
    } else if (action === 'remove-affiliate') {
        group.affiliates = group.affiliates.filter(id => id !== targetGroup.id);
        if(targetGroup) targetGroup.affiliates = targetGroup.affiliates.filter(id => id !== group.id);
    } else if (action === 'declare-enemy') {
        if (!targetGroup.allowEnemies) return res.status(403).json({ error: 'This group does not accept enemies.' });
        if (!group.enemies.includes(targetGroup.id)) group.enemies.push(targetGroup.id);
    } else if (action === 'remove-enemy') {
        group.enemies = group.enemies.filter(id => id !== targetGroup.id);
    }

    saveDB();
    res.json({ success: true });
});

app.get('/api/groups/:id/wall', (req, res) => {
    const group = db.groups.find(gr => gr.id === req.params.id);
    if (!group) return res.status(404).json({ error: 'Group not found.' });
    res.json(group.posts.slice(0, 50));
});

app.post('/api/groups/:id/join', requireAuth, (req, res) => {
    const group = db.groups.find(gr => gr.id === req.params.id);
    if (!group) return res.status(404).json({ error: 'Group not found.' });
    if (group.banned.includes(req.userId)) return res.status(403).json({ error: 'You are banned from this group.' });
    
    if (!group.members.find(m => m.userId === req.userId)) {
        const defRole = group.roles.find(r => r.rank === 1) || group.roles[group.roles.length-1];
        group.members.push({ userId: req.userId, roleId: defRole.id, joinedAt: Date.now() });
        saveDB();
    }
    res.json({ message: 'Joined group!' });
});

app.post('/api/groups/:id/leave', requireAuth, (req, res) => {
    const group = db.groups.find(gr => gr.id === req.params.id);
    if (!group) return res.status(404).json({ error: 'Group not found.' });
    const member = group.members.find(m => m.userId === req.userId);
    if (member && group.ownerId === req.userId) return res.status(400).json({ error: 'Owner cannot leave group.' });
    
    group.members = group.members.filter(m => m.userId !== req.userId);
    saveDB();
    res.json({ message: 'Left group.' });
});

app.post('/api/groups/:id/posts', requireAuth, (req, res) => {
    const { text } = req.body;
    if (!text || text.trim().length === 0) return res.status(400).json({ error: 'Post cannot be empty.' });

    const group = db.groups.find(gr => gr.id === req.params.id);
    if (!group) return res.status(404).json({ error: 'Group not found.' });

    if (!group.members.find(m => m.userId === req.userId)) {
        return res.status(403).json({ error: 'Must be a member to post.' });
    }

    const user = db.users.find(u => u.id === req.userId);
    group.posts.unshift({
        id: crypto.randomUUID(), authorName: user.username, authorId: user.id, text: text.trim().substring(0, 200), timestamp: Date.now()
    });

    addGroupXp(group, 5); // Earn XP for posting
    saveDB();
    res.json({ message: 'Posted successfully!', posts: group.posts.slice(0, 50) });
});

app.delete('/api/groups/:id/posts/:postId', requireAuth, (req, res) => {
    const group = db.groups.find(gr => gr.id === req.params.id);
    if (!group) return res.status(404).json({ error: 'Group not found.' });

    const perms = getGroupMemberPerms(group, req.userId);
    if (!perms || !perms.deletePosts) return res.status(403).json({ error: 'Permission denied.' });

    group.posts = group.posts.filter(p => p.id !== req.params.postId);
    saveDB();
    res.json({ success: true });
});

app.post('/api/me/claim-login', requireAuth, (req, res) => {
    const user = db.users.find(u => u.id === req.userId);
    const todayStr = new Date().toDateString();
    const lastLoginStr = user.lastLoginDate ? new Date(user.lastLoginDate).toDateString() : '';

    if (todayStr === lastLoginStr) return res.status(400).json({ error: 'Already claimed today.' });

    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    
    if (lastLoginStr === yesterday.toDateString()) user.loginStreak += 1;
    else user.loginStreak = 1; // Reset streak if missed

    user.lastLoginDate = Date.now();
    let reward = (user.loginStreak % 7 === 0) ? 150 : 50;
    
    user.coins += reward;
    saveDB();
    res.json({ success: true, coins: user.coins, streak: user.loginStreak, reward });
});

// Admin endpoints
app.post('/api/groups/:id/roles', requireAuth, (req, res) => {
    const group = db.groups.find(gr => gr.id === req.params.id);
    const perms = getGroupMemberPerms(group, req.userId);
    if (!perms || !perms.manageRanks) return res.status(403).json({ error: 'Permission denied.' });

    const { name, rank, permissions } = req.body;
    if (rank >= 255) return res.status(400).json({ error: 'Cannot create a role equal to or higher than Owner.' });
    
    const role = { id: crypto.randomUUID(), name, rank: parseInt(rank) || 10, perms: permissions || {} };
    group.roles.push(role);
    group.roles.sort((a,b) => b.rank - a.rank);
    saveDB();
    res.json({ success: true, roles: group.roles });
});

app.put('/api/groups/:id/members/:userId', requireAuth, (req, res) => {
    const group = db.groups.find(gr => gr.id === req.params.id);
    const perms = getGroupMemberPerms(group, req.userId);
    const myRank = getGroupMemberRank(group, req.userId);
    if (!perms || !perms.manageRanks) return res.status(403).json({ error: 'Permission denied.' });

    const { roleId } = req.body;
    const targetRank = group.roles.find(r => r.id === roleId)?.rank || 0;
    if (targetRank >= myRank) return res.status(403).json({ error: 'Cannot assign a rank equal to or higher than your own.' });

    const targetMem = group.members.find(m => m.userId === req.params.userId);
    const targetCurrentRank = getGroupMemberRank(group, req.params.userId);
    if (targetCurrentRank >= myRank) return res.status(403).json({ error: 'Cannot modify a member with equal or higher rank.' });

    if (targetMem) targetMem.roleId = roleId;
    saveDB();
    res.json({ success: true });
});

app.post('/api/groups/:id/kick/:userId', requireAuth, (req, res) => {
    const group = db.groups.find(gr => gr.id === req.params.id);
    const perms = getGroupMemberPerms(group, req.userId);
    const myRank = getGroupMemberRank(group, req.userId);
    if (!perms || !perms.kick) return res.status(403).json({ error: 'Permission denied.' });

    const targetCurrentRank = getGroupMemberRank(group, req.params.userId);
    if (targetCurrentRank >= myRank) return res.status(403).json({ error: 'Cannot kick a member with equal or higher rank.' });

    group.members = group.members.filter(m => m.userId !== req.params.userId);
    saveDB();
    res.json({ success: true });
});

app.post('/api/groups/:id/categories', requireAuth, (req, res) => {
    const group = db.groups.find(gr => gr.id === req.params.id);
    const perms = getGroupMemberPerms(group, req.userId);
    if (!perms || !perms.manageCategories) return res.status(403).json({ error: 'Permission denied.' });
    if (group.categories.length >= 15) return res.status(400).json({ error: 'Max 15 categories allowed.' });

    const { title, description } = req.body;
    group.categories.push({ id: crypto.randomUUID(), title, description: description || '' });
    saveDB();
    res.json({ success: true, categories: group.categories });
});

app.get('/api/groups/:id/forums/:catId', (req, res) => {
    const group = db.groups.find(gr => gr.id === req.params.id);
    if (!group) return res.status(404).json({ error: 'Group not found.' });
    const threads = group.threads.filter(t => t.categoryId === req.params.catId).map(t => ({
        id: t.id, title: t.title, authorName: t.authorName, repliesCount: (t.replies || []).length, timestamp: t.timestamp
    })).sort((a,b) => b.timestamp - a.timestamp);
    res.json(threads);
});

app.post('/api/groups/:id/forums/:catId', requireAuth, (req, res) => {
    const group = db.groups.find(gr => gr.id === req.params.id);
    if (!group.members.find(m => m.userId === req.userId)) return res.status(403).json({ error: 'Members only.' });

    const user = db.users.find(u => u.id === req.userId);
    const { title, content } = req.body;
    const thread = {
        id: crypto.randomUUID(), categoryId: req.params.catId, authorId: user.id, authorName: user.username,
        title, content, timestamp: Date.now(), replies: []
    };
    group.threads.push(thread);
    addGroupXp(group, 5); // XP for posting thread
    saveDB();
    res.json({ success: true });
});

app.get('/api/groups/:id/threads/:threadId', (req, res) => {
    const group = db.groups.find(gr => gr.id === req.params.id);
    const thread = group.threads.find(t => t.id === req.params.threadId);
    if (!thread) return res.status(404).json({ error: 'Thread not found.' });
    res.json(thread);
});

app.post('/api/groups/:id/threads/:threadId/replies', requireAuth, (req, res) => {
    const group = db.groups.find(gr => gr.id === req.params.id);
    if (!group.members.find(m => m.userId === req.userId)) return res.status(403).json({ error: 'Members only.' });

    const user = db.users.find(u => u.id === req.userId);
    const thread = group.threads.find(t => t.id === req.params.threadId);
    if (!thread) return res.status(404).json({ error: 'Thread not found.' });

    thread.replies.push({
        id: crypto.randomUUID(), authorId: user.id, authorName: user.username, content: req.body.content, timestamp: Date.now()
    });
    addGroupXp(group, 5); // XP for reply
    saveDB();
    res.json({ success: true, replies: thread.replies });
});

app.post('/api/groups/:id/events', requireAuth, (req, res) => {
    const group = db.groups.find(gr => gr.id === req.params.id);
    const perms = getGroupMemberPerms(group, req.userId);
    if (!perms || !perms.manageEvents) return res.status(403).json({error: 'Permission denied.'});
    
    const { name, description, datetime } = req.body;
    if (!group.events) group.events = [];
    group.events.push({ 
        id: crypto.randomUUID(), name, description, datetime, 
        authorName: db.users.find(u=>u.id===req.userId).username, timestamp: Date.now() 
    });
    saveDB();
    res.json({ success: true, events: group.events });
});

app.post('/api/groups/:id/payout', requireAuth, (req, res) => {
    const group = db.groups.find(gr => gr.id === req.params.id);
    const perms = getGroupMemberPerms(group, req.userId);
    if (!perms || !perms.managePayouts) return res.status(403).json({error: 'Permission denied.'});
    
    const { targetUserId, amount } = req.body;
    const amt = parseInt(amount);
    if (!amt || amt <= 0 || group.coins < amt) return res.status(400).json({error: 'Invalid amount or insufficient group funds.'});
    
    const targetUser = db.users.find(u => u.id === targetUserId);
    if (!targetUser) return res.status(404).json({error: 'User not found.'});
    
    group.coins -= amt;
    targetUser.coins += amt;
    saveDB();
    res.json({ success: true, groupCoins: group.coins });
});


// --- Shop & Economy Routes ---

app.get('/api/shop/items', (req, res) => {
    res.json(db.shopItems.sort((a,b) => new Date(b.createdAt) - new Date(a.createdAt)));
});

app.post('/api/shop/items', requireAuth, (req, res) => {
    const { name, description, price, image } = req.body;
    if (!name || !image) return res.status(400).json({ error: 'Missing required data.' });

    const user = db.users.find(u => u.id === req.userId);
    if (user.coins < 15) return res.status(400).json({ error: 'Insufficient Funds.' });
    user.coins -= 15;

    const newItem = {
        id: crypto.randomUUID(), name, description: description || '', price: parseInt(price) || 0,
        authorId: user.id, authorName: user.username, image, createdAt: new Date().toISOString()
    };
    
    db.shopItems.push(newItem);
    user.inventory.push(newItem.id); 
    saveDB();
    
    res.json({ message: 'Accessory published successfully!', item: newItem, coins: user.coins });
});

app.post('/api/shop/buy/:id', requireAuth, (req, res) => {
    const item = db.shopItems.find(i => i.id === req.params.id);
    const user = db.users.find(u => u.id === req.userId);
    
    if (!item) return res.status(404).json({ error: 'Item not found.' });
    if (user.inventory.includes(item.id)) return res.status(400).json({ error: 'You already own this item.' });
    if (user.coins < item.price) return res.status(400).json({ error: 'Insufficient Funds.' });
    
    user.coins -= item.price;
    user.inventory.push(item.id);
    saveDB();
    res.json({ message: 'Item purchased successfully!', coins: user.coins });
});

// --- Game Routes ---
// Get Games Library (Search & Filter)
app.get('/api/games', (req, res) => {
    let results = [...db.games];
    if (req.query.q) {
        const q = req.query.q.toLowerCase();
        results = results.filter(g => g.title.toLowerCase().includes(q) || g.authorName.toLowerCase().includes(q));
    }
    if (req.query.genre && req.query.genre !== 'All') {
        results = results.filter(g => g.genre === req.query.genre);
    }
    results.sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime());
    const list = results.map(g => ({
        id: g.id, title: g.title, authorName: g.authorName, genre: g.genre, likes: g.likes.length, plays: g.plays, groupId: g.groupId
    }));
    res.json(list);
});


// --- PLAYSCULPT DATASTORES ---
app.post('/api/games/:id/datastore', requireAuth, (req, res) => {
    const { key, value } = req.body;
    if (!db.datastores[req.params.id]) db.datastores[req.params.id] = {};
    if (!db.datastores[req.params.id][req.userId]) db.datastores[req.params.id][req.userId] = {};
    
    db.datastores[req.params.id][req.userId][key] = value;
    saveDB();
    res.json({ success: true });
});

app.get('/api/games/:id/datastore/:key', requireAuth, (req, res) => {
    const val = db.datastores[req.params.id]?.[req.userId]?.[req.params.key];
    res.json({ value: val !== undefined ? val : null });
});



// 2. Publish New Version
app.post('/api/games/:id/publish', requireAuth, (req, res) => {
    const game = db.games.find(g => g.id === req.params.id);
    if (!game) return res.status(404).json({ error: 'Game not found.' });

    let canEdit = game.authorId === req.userId || game.collaborators.includes(req.userId);
    if (game.groupId) {
        const group = db.groups.find(gr => gr.id === game.groupId);
        const perms = getGroupMemberPerms(group, req.userId);
        if (perms && perms.editGames) canEdit = true;
    }
    if (!canEdit) return res.status(403).json({ error: 'Not authorized.' });

    const { gameData, genre } = req.body;
    game.gameData = gameData;
    if (genre) game.genre = genre;
    game.lastEditTime = Date.now();

    if (!game.versions) game.versions = [];
    game.versions.push({ versionId: game.versions.length + 1, timestamp: Date.now(), gameData: JSON.parse(JSON.stringify(gameData)) });

    saveDB();
    res.json({ success: true, versionId: game.versions.length });
});


// 3. Fetch Version List (Metadata Only)
app.get('/api/games/:id/versions', requireAuth, (req, res) => {
    const game = db.games.find(g => g.id === req.params.id);
    if (!game) return res.status(404).json({ error: 'Game not found.' });
    const versionList = (game.versions || []).map(v => ({ versionId: v.versionId, timestamp: v.timestamp })).reverse();
    res.json(versionList);
});

// 4. Fetch Specific Version Data
app.get('/api/games/:id/versions/:vId', requireAuth, (req, res) => {
    const game = db.games.find(g => g.id === req.params.id);
    if (!game) return res.status(404).json({ error: 'Game not found.' });
    const v = game.versions.find(ver => ver.versionId === parseInt(req.params.vId));
    if (!v) return res.status(404).json({ error: 'Version not found.' });
    res.json({ gameData: v.gameData });
});

// 5. Sound Service Routes
app.post('/api/sounds', requireAuth, (req, res) => {
    const { name, data } = req.body;
    if (!name || !data) return res.status(400).json({ error: 'Missing data' });
    const user = db.users.find(u => u.id === req.userId);
    const newSound = { id: crypto.randomUUID(), name, data, authorId: user.id, authorName: user.username, createdAt: Date.now() };
    db.sounds.push(newSound);
    saveDB();
    res.json({ success: true, soundId: newSound.id });
});

app.get('/api/sounds', (req, res) => {
    res.json(db.sounds.map(s => ({ id: s.id, name: s.name, authorName: s.authorName })).reverse());
});

app.get('/api/sounds/:id', (req, res) => {
    const sound = db.sounds.find(s => s.id === req.params.id);
    if (!sound) return res.status(404).json({ error: 'Not found' });
    res.json({ data: sound.data });
});



app.get('/api/games/most-liked', (req, res) => {
    const mostLiked = [...db.games]
        .sort((a, b) => b.likes.length - a.likes.length)
        .slice(0, 4)
        .map(g => ({ id: g.id, title: g.title, authorName: g.authorName, genre: g.genre, likes: g.likes.length, plays: g.plays, groupId: g.groupId }));
    res.json(mostLiked);
});

app.get('/api/games/fresh', (req, res) => {
    const fresh = [...db.games]
        .sort((a, b) => {
            const tA = a.lastEditTime || new Date(a.createdAt).getTime();
            const tB = b.lastEditTime || new Date(b.createdAt).getTime();
            return tB - tA;
        })
        .slice(0, 4)
        .map(g => ({ id: g.id, title: g.title, authorName: g.authorName, genre: g.genre, likes: g.likes.length, plays: g.plays, groupId: g.groupId }));
    res.json(fresh);
});

app.get('/api/my-games', requireAuth, (req, res) => {
    const userGroups = db.groups.filter(gr => {
        const perms = getGroupMemberPerms(gr, req.userId);
        return perms && perms.editGames;
    });
    const groupIds = userGroups.map(gr => gr.id);

    const myGames = db.games.filter(g => 
        g.authorId === req.userId || 
        g.collaborators.includes(req.userId) ||
        (g.groupId && groupIds.includes(g.groupId))
    ).map(g => ({
        id: g.id, title: g.title, authorName: g.authorName, genre: g.genre, likes: g.likes.length, plays: g.plays, groupId: g.groupId
    }));
    res.json(myGames);
});

app.get('/api/games/trending', (req, res) => {
    // Sort games by highest play count
    const trending = [...db.games]
        .sort((a, b) => (b.plays || 0) - (a.plays || 0))
        .slice(0, 4)
        .map(g => ({ 
            id: g.id, 
            title: g.title, 
            authorName: g.authorName, 
            genre: g.genre, 
            likes: g.likes.length, 
            plays: g.plays, 
            groupId: g.groupId 
        }));
    res.json(trending);
});


// PUBLISH NEW GAME
app.post('/api/games', requireAuth, (req, res) => {
    const { title, gameData, genre, groupId, icon, thumbnails } = req.body;
    if (!title || !gameData) return res.status(400).json({ error: 'Missing game data.' });
    
    const user = db.users.find(u => u.id === req.userId);
    let authorName = user.username;

    if (groupId) {
        const group = db.groups.find(gr => gr.id === groupId);
        const perms = getGroupMemberPerms(group, req.userId);
        if (!group || !perms || !perms.editGames) return res.status(403).json({ error: 'Not authorized.' });
        authorName = group.name; 
    }

    const newGame = {
        id: crypto.randomUUID(), title, authorId: user.id, authorName: authorName, genre: genre || 'Sandbox',
        groupId: groupId || null, gameData, lastEditTime: Date.now(), collaborators: [], likes: [], plays: 0, updates: [],
        createdAt: new Date().toISOString(),
        versions: [{ versionId: 1, timestamp: Date.now(), gameData }],
        analytics: { uniquePlayers: [], totalSessionTimeSeconds: 0, fallOffs: 0, peakCCU: 0, desktopSessions: 0, mobileSessions: 0, totalJumps: 0 },
        
        // NEW: Image fields
        icon: null, // Active square icon
        pendingIcon: icon || null, // Awaiting admin approval
        thumbnails: [], // Active horizontal thumbnails
        pendingThumbnails: (thumbnails || []).slice(0, 10).map(t => ({ id: crypto.randomUUID(), data: t })) // Max 10
    };
    db.games.push(newGame);
    saveDB();
res.json({ success: true, gameId: newGame.id, message: "Game published! Images are awaiting Admin approval." });
});

app.post('/api/me/lucky-spin', requireAuth, (req, res) => {
    const user = db.users.find(u => u.id === req.userId);
    const WEEK_MS = 7 * 24 * 60 * 60 * 1000;
    
    // Check if 7 days have passed since the last spin
    if (Date.now() - (user.lastSpinDate || 0) < WEEK_MS) {
        return res.status(400).json({ error: 'You can only spin once a week!' });
    }

    const rewards = [
        { type: 'coins', val: 100, label: '100 SC' },
        { type: 'coins', val: 200, label: '200 SC' },
        { type: 'coins', val: 350, label: '350 SC' },
        { type: 'badge', val: 'Lucky Spinner', label: 'Lucky Spinner Badge' }
    ];
    
    // Weighted Randomness (10% chance for badge, 40% chance for 100 SC, etc.)
    const r = Math.random();
    let reward;
    if (r < 0.4) reward = rewards[0]; 
    else if (r < 0.7) reward = rewards[1]; 
    else if (r < 0.9) reward = rewards[2]; 
    else reward = rewards[3]; 

    if (reward.type === 'coins') {
        user.coins += reward.val;
    } else if (reward.type === 'badge') {
        if (!user.badges.includes(reward.val)) {
            user.badges.push(reward.val);
        } else {
            // If they already have the badge, give them a jackpot of SC instead!
            user.coins += 150;
            reward = { type: 'coins', val: 150, label: '150 SC (Badge Duplicate)' };
        }
    }

    user.lastSpinDate = Date.now();
    saveDB();
    res.json({ success: true, reward, coins: user.coins });
});

app.get('/api/games/random', (req, res) => {
    if (db.games.length === 0) return res.status(404).json({ error: 'No games found.' });
    const randomGame = db.games[Math.floor(Math.random() * db.games.length)];
    res.json({ id: randomGame.id });
});


// Admin Image Moderation Queue
app.get('/api/moderate/images', requireAuth, (req, res) => {
    const adminUser = db.users.find(u => u.id === req.userId);
    if (!adminUser || adminUser.username.toLowerCase() !== 'admin') return res.status(403).json({ error: 'Admins only.' });
    
    let queue = [];
    db.games.forEach(g => {
        if (g.pendingIcon) queue.push({ type: 'icon', gameId: g.id, gameTitle: g.title, data: g.pendingIcon });
        if (g.pendingThumbnails) {
            g.pendingThumbnails.forEach(t => queue.push({ type: 'thumbnail', gameId: g.id, imageId: t.id, gameTitle: g.title, data: t.data }));
        }
    });
    res.json(queue);
});

app.post('/api/moderate/images', requireAuth, (req, res) => {
    const adminUser = db.users.find(u => u.id === req.userId);
    if (!adminUser || adminUser.username.toLowerCase() !== 'admin') return res.status(403).json({ error: 'Admins only.' });
    
    const { gameId, type, imageId, action } = req.body; // action: 'approve' or 'deny'
    const game = db.games.find(g => g.id === gameId);
    if (!game) return res.status(404).json({ error: 'Game not found' });

    if (type === 'icon') {
        if (action === 'approve') game.icon = game.pendingIcon;
        game.pendingIcon = null;
    } else if (type === 'thumbnail') {
        const thumb = game.pendingThumbnails.find(t => t.id === imageId);
        if (thumb && action === 'approve') game.thumbnails.push(thumb.data);
        game.pendingThumbnails = game.pendingThumbnails.filter(t => t.id !== imageId);
    }
    
    saveDB();
    res.json({ success: true });
});

app.get('/api/games/:id', (req, res) => {
    const game = db.games.find(g => g.id === req.params.id);
    if (!game) return res.status(404).json({ error: 'Game not found.' });
    
    let isLiked = false;
    let isBookmarked = false;
    const token = req.headers.authorization;
    if (token && db.sessions[token]) {
        const userId = db.sessions[token];
        if (game.likes.includes(userId)) isLiked = true;
        const user = db.users.find(u => u.id === userId);
        if (user && user.bookmarks.includes(game.id)) isBookmarked = true;
    }
    res.json({ ...game, likesCount: game.likes.length, isLiked, isBookmarked, updates: game.updates || [] });
});

app.post('/api/games/:id/updates', requireAuth, (req, res) => {
    const game = db.games.find(g => g.id === req.params.id);
    if (!game) return res.status(404).json({ error: 'Game not found.' });
    
    let canEdit = game.authorId === req.userId || game.collaborators.includes(req.userId);
    if (game.groupId) {
        const group = db.groups.find(gr => gr.id === game.groupId);
        const perms = getGroupMemberPerms(group, req.userId);
        if (perms && perms.editGames) canEdit = true;
    }

    if (!canEdit) return res.status(403).json({ error: 'Not authorized to post updates.' });
    if (!req.body.text || req.body.text.trim().length === 0) return res.status(400).json({ error: 'Update text cannot be empty.' });

    if (!game.updates) game.updates = [];
    game.updates.unshift({ text: req.body.text.trim().substring(0, 200), timestamp: Date.now() });
    
    saveDB();
    res.json({ success: true, updates: game.updates });
});


// ==========================================
// LIVE ONLINE TRACKING
// ==========================================
app.post('/api/ping', requireAuth, (req, res) => {
    const { location } = req.body;
    
    // Update this user's last seen time and location
    onlineUsers[req.userId] = {
        lastSeen: Date.now(),
        location: location || 'website'
    };

    const now = Date.now();
    let totalOnline = 0;
    let cityOnline = 0;

    // Count active users and kick out AFK/Disconnected users (no ping for 30 seconds)
    for (let uid in onlineUsers) {
        if (now - onlineUsers[uid].lastSeen > 30000) {
            delete onlineUsers[uid];
        } else {
            totalOnline++;
            if (onlineUsers[uid].location === 'city') cityOnline++;
        }
    }

    res.json({ totalOnline, cityOnline });
});

// Analytics Data Receiver
// Analytics Data Receiver
app.post('/api/games/:id/track', requireAuth, (req, res) => {
    const game = db.games.find(g => g.id === req.params.id);
    if (!game) return res.status(404).json({ error: 'Game not found.' });

    // FIX: Initialize analytics if it doesn't exist (prevents the 500 crash)
    if (!game.analytics) {
        game.analytics = {
            uniquePlayers: [],
            totalSessionTimeSeconds: 0,
            fallOffs: 0,
            peakCCU: 0,
            desktopSessions: 0,
            mobileSessions: 0,
            totalJumps: 0
        };
    }

    const { sessionTimeSeconds, jumps, falls, isMobile } = req.body;
    
    // Add unique player if not already tracked
    if (!game.analytics.uniquePlayers.includes(req.userId)) {
        game.analytics.uniquePlayers.push(req.userId);
    }
    
    // Aggregate metrics
    game.analytics.totalSessionTimeSeconds += (sessionTimeSeconds || 0);
    game.analytics.totalJumps += (jumps || 0);
    game.analytics.fallOffs += (falls || 0);
    
    if (isMobile) game.analytics.mobileSessions += 1;
    else game.analytics.desktopSessions += 1;
    
    // Update Peak CCU 
    if (activePlayers[game.id]) {
        const currentCCU = Object.keys(activePlayers[game.id]).length;
        if (currentCCU > game.analytics.peakCCU) {
            game.analytics.peakCCU = currentCCU;
        }
    }

    saveDB();
    res.json({ success: true });
});

// Analytics Fetch Endpoint (Restricted to Creators & Group Admins)
app.get('/api/games/:id/analytics', requireAuth, (req, res) => {
    const game = db.games.find(g => g.id === req.params.id);
    if (!game) return res.status(404).json({ error: 'Game not found.' });
    
    let canView = game.authorId === req.userId;
    if (game.groupId) {
        const group = db.groups.find(gr => gr.id === game.groupId);
        const mem = group?.members.find(m => m.userId === req.userId);
        const role = group?.roles.find(r => r.id === mem?.roleId);
        if (role && role.perms.editGames) canView = true;
    }
    
    if (!canView) return res.status(403).json({ error: 'Not authorized.' });

    res.json({ plays: game.plays, likes: game.likes.length, analytics: game.analytics });
});


// --- SCULPT CITY ROUTES ---
// ==========================================
// SCULPT CITY ROUTES
// ==========================================
app.get('/api/city/info', requireAuth, (req, res) => {
    const user = db.users.find(u => u.id === req.userId);
    if (user.cityData) {
        if (!user.cityData.bucks) user.cityData.bucks = 0;
        if (!user.cityData.vehicles) user.cityData.vehicles = ['sedan_1'];
    }
    res.json({ cityData: user.cityData, plots: db.cityPlots || [] });
});

app.post('/api/city/claim', requireAuth, (req, res) => {
    const { neighborhood, plotX, plotZ } = req.body;
    const user = db.users.find(u => u.id === req.userId);
    
    if (user.cityData) return res.status(400).json({ error: 'You already claimed a plot!' });
    if (!db.cityPlots) db.cityPlots = [];
    
    const isTaken = db.cityPlots.find(p => p.plotX === plotX && p.plotZ === plotZ && p.neighborhood === neighborhood);
    if (isTaken) return res.status(400).json({ error: 'Plot is already taken!' });

    // Initialize the player with a Starter House, 0 Bucks, and a Sedan!
    user.cityData = { neighborhood, plotX, plotZ, houseType: 'Starter', bucks: 0, vehicles: ['sedan_1'] };
    db.cityPlots.push({
        id: crypto.randomUUID(), userId: user.id, username: user.username,
        neighborhood, plotX, plotZ, houseType: 'Starter'
    });

    saveDB();
    res.json({ success: true, cityData: user.cityData });
});

// NEW: Economy Sync (Rewards & Vehicle Purchases)
// NEW: Economy Sync (Rewards & Vehicle Purchases)
app.post('/api/city/sync', requireAuth, (req, res) => {
    const user = db.users.find(u => u.id === req.userId);
    
    // Grant Coins
    if (req.body.coinsToAdd) user.coins = (user.coins || 0) + req.body.coinsToAdd;
    
    // Grant Bucks
    if (user.cityData && req.body.bucksToAdd) {
        user.cityData.bucks = (user.cityData.bucks || 0) + req.body.bucksToAdd;
    }
    
    // Process Vehicle Purchases
    if (user.cityData && req.body.vehicleToBuy) {
        if (!user.cityData.vehicles) user.cityData.vehicles = ['sedan_1'];
        if (user.cityData.bucks >= req.body.cost) {
            user.cityData.bucks -= req.body.cost;
            user.cityData.vehicles.push(req.body.vehicleToBuy);
        } else {
            return res.status(400).json({error: 'Not enough Bucks!'});
        }
    }
    
    saveDB();
    res.json({ coins: user.coins, cityData: user.cityData });
});

app.post('/api/games/:id/play', requireAuth, (req, res) => {
    const user = db.users.find(u => u.id === req.userId);
    const gameId = req.params.id;
    const game = db.games.find(g => g.id === gameId);
    
    if (game) {
        game.plays = (game.plays || 0) + 1;
        if (game.groupId) {
            const group = db.groups.find(gr => gr.id === game.groupId);
            if (group) { group.coins = (group.coins || 0) + 5; addGroupXp(group, 10); }
        }
    }

    // PLAY STREAK MATH
    let streakReward = 0;
    const todayStr = new Date().toDateString();
    const lastPlayStr = user.lastPlayDate ? new Date(user.lastPlayDate).toDateString() : '';
    
    if (todayStr !== lastPlayStr) {
        const yesterday = new Date(); yesterday.setDate(yesterday.getDate() - 1);
        if (lastPlayStr === yesterday.toDateString()) user.playStreak += 1;
        else user.playStreak = 1;
        
        user.lastPlayDate = Date.now();
        if (user.playStreak > 0 && user.playStreak % 2 === 0) streakReward = 15;
    }

    user.coins += 5 + streakReward;
    user.recentlyPlayed = user.recentlyPlayed.filter(g => g.gameId !== gameId);
    user.recentlyPlayed.unshift({ gameId, timestamp: Date.now() });
    if (user.recentlyPlayed.length > 8) user.recentlyPlayed.pop();

    awardBadge(req.userId, 'Gamer');
    saveDB();
    res.json({ success: true, coins: user.coins, streakReward, playStreak: user.playStreak });
});
app.post('/api/games/:id/like', requireAuth, (req, res) => {
    const game = db.games.find(g => g.id === req.params.id);
    if (!game) return res.status(404).json({ error: 'Game not found.' });

    let isLiked = false;
    if (game.likes.includes(req.userId)) {
        game.likes = game.likes.filter(id => id !== req.userId);
    } else {
        game.likes.push(req.userId);
        isLiked = true;
        awardBadge(req.userId, 'Critic');
    }
    saveDB();
    res.json({ likesCount: game.likes.length, isLiked });
});

app.post('/api/games/:id/bookmark', requireAuth, (req, res) => {
    const user = db.users.find(u => u.id === req.userId);
    const gameId = req.params.id;
    if (!db.games.find(g => g.id === gameId)) return res.status(404).json({ error: 'Game not found.' });

    if (!user.bookmarks) user.bookmarks = [];

    let isBookmarked = false;
    if (user.bookmarks.includes(gameId)) {
        user.bookmarks = user.bookmarks.filter(id => id !== gameId);
    } else {
        user.bookmarks.push(gameId);
        isBookmarked = true;
    }
    saveDB();
    res.json({ isBookmarked });
});

app.post('/api/games/:id/collaborators', requireAuth, (req, res) => {
    const game = db.games.find(g => g.id === req.params.id);
    if (!game) return res.status(404).json({ error: 'Game not found.' });
    if (game.authorId !== req.userId) return res.status(403).json({ error: 'Only creator can add collaborators.' });

    const { username } = req.body;
    const colUser = db.users.find(u => u.username.toLowerCase() === username.toLowerCase());
    if (!colUser) return res.status(404).json({ error: 'User not found.' });
    if (colUser.id === req.userId) return res.status(400).json({ error: 'You already own this game.' });

    if (!game.collaborators.includes(colUser.id)) {
        game.collaborators.push(colUser.id);
        saveDB();
    }
    res.json({ message: `${colUser.username} added as a collaborator!` });
});

app.post('/api/games/:id/sync', requireAuth, (req, res) => {
    const game = db.games.find(g => g.id === req.params.id);
    if (!game) return res.status(404).json({ error: 'Game not found.' });
    
    let canEdit = game.authorId === req.userId || game.collaborators.includes(req.userId);
    if (game.groupId) {
        const group = db.groups.find(gr => gr.id === game.groupId);
        const perms = getGroupMemberPerms(group, req.userId);
        if (perms && perms.editGames) canEdit = true;
    }

    if (!canEdit) return res.status(403).json({ error: 'Not authorized.' });

    const { gameData, genre, lastLocalEditTime, cursor } = req.body;
    if (!activeEditors[game.id]) activeEditors[game.id] = {};
    
    // Store their timestamp AND their 3D cursor position
    activeEditors[game.id][req.userId] = {
        timestamp: Date.now(),
        cursor: cursor || null
    };

    const activeUsernames = [];
    const activeEditorsData = []; // Holds the 3D data of other players
    
    for (let uId in activeEditors[game.id]) {
        if (Date.now() - activeEditors[game.id][uId].timestamp < 4000) {
            const u = db.users.find(usr => usr.id === uId);
            if (u) {
                activeUsernames.push(u.username);
                // Send cursor data to everyone EXCEPT the user requesting it
                if (uId !== req.userId && activeEditors[game.id][uId].cursor) {
                    activeEditorsData.push({ username: u.username, cursor: activeEditors[game.id][uId].cursor });
                }
            }
        } else delete activeEditors[game.id][uId];
    }

    let appliedUpdate = false;
    if (gameData && lastLocalEditTime > game.lastEditTime) {
        game.gameData = gameData;
        if (genre) game.genre = genre;
        game.lastEditTime = lastLocalEditTime;
        saveDB(); appliedUpdate = true;
    }

    // Return the new activeEditorsData array
    res.json({ gameData: game.gameData, genre: game.genre, lastEditTime: game.lastEditTime, activeEditors: activeUsernames, activeEditorsData, acceptedLocalUpdate: appliedUpdate });
});

app.post("/api/invite", requireAuth, (req, res) => {
    const { friendId, gameId } = req.body;

    const sender = db.users.find(u => u.id === req.userId);
    const game = db.games.find(g => g.id === gameId);
    const target = db.users.find(u => u.id === friendId);

    if (!sender) return res.status(400).json({ error: "Sender not found" });
    if (!game) return res.status(400).json({ error: "Game not found" });
    if (!target) return res.status(400).json({ error: "Friend not found" });

    const key = `${sender.id}_${friendId}`;
    const now = Date.now();

    // ⏱️ 3 minute cooldown (180000 ms)
    if (inviteCooldowns[key] && now - inviteCooldowns[key] < 180000) {
        const remaining = Math.ceil((180000 - (now - inviteCooldowns[key])) / 1000);
        return res.status(429).json({
            error: `You can invite this player again in ${remaining}s`
        });
    }

    // ✅ save cooldown
    inviteCooldowns[key] = now;

    // ✅ create notification
    createNotification(friendId, "invite", {
        from: sender.username,
        fromId: sender.id,
        gameId: game.id,
        gameName: game.title || "Unknown Game"
    });

    res.json({ success: true });
});


// ==========================================
// GLOBAL CHAT & SPAM PROTECTION
// ==========================================
app.get('/api/chat', (req, res) => {
    res.json((db.globalChat || []).slice(-50)); // Return last 50 messages
});

app.post('/api/chat', requireAuth, (req, res) => {
    const user = db.users.find(u => u.id === req.userId);
    const text = (req.body.text || '').trim().substring(0, 150);
    if (!text) return res.status(400).json({error: 'Message cannot be empty.'});

    // 1. Account Age Check (30 minutes = 1,800,000 ms)
    const age = Date.now() - (user.createdAt || 0);
    if (age < 1800000) {
        const minsLeft = Math.ceil((1800000 - age) / 60000);
        return res.status(403).json({error: `Account must be 30 minutes old to chat. (${minsLeft} mins remaining)`});
    }

    // 2. Suspension Check
    if (chatSuspensions[req.userId] && Date.now() < chatSuspensions[req.userId]) {
        const secs = Math.ceil((chatSuspensions[req.userId] - Date.now()) / 1000);
        return res.status(403).json({error: `Chat suspended for spamming. Try again in ${secs}s.`});
    }

    // 3. Spam Detection (Max 3 messages in 5 seconds)
    if (!chatActivity[req.userId]) chatActivity[req.userId] = [];
    const now = Date.now();
    chatActivity[req.userId] = chatActivity[req.userId].filter(t => now - t < 5000);
    chatActivity[req.userId].push(now);

    if (chatActivity[req.userId].length > 3) { 
        chatSuspensions[req.userId] = now + 15000; // 15-second suspension
        return res.status(403).json({error: 'Spam detected. Chat suspended for 15 seconds.'});
    }

    if (!db.globalChat) db.globalChat = [];
    const newMsg = { id: crypto.randomUUID(), authorName: user.username, text, timestamp: now };
    
    db.globalChat.push(newMsg);
    if (db.globalChat.length > 100) db.globalChat.shift(); // Keep memory clean
    
    res.json({ success: true, message: newMsg });
});


// ==========================================
// GAME SERVER CHAT
// ==========================================
app.get('/api/games/:id/chat', requireAuth, (req, res) => {
    const gameId = req.params.id;
    res.json({ messages: (gameChats[gameId] || []).slice(-50) });
});

app.post('/api/games/:id/chat', requireAuth, (req, res) => {
    const gameId = req.params.id;
    const user = db.users.find(u => u.id === req.userId);
    const text = String(req.body.text || '').trim().substring(0, 150);

    if (!text) {
        return res.status(400).json({ error: 'Message cannot be empty.' });
    }

    const key = `${gameId}_${req.userId}`;
    const now = Date.now();

    if (gameChatSuspensions[key] && now < gameChatSuspensions[key]) {
        const secs = Math.ceil((gameChatSuspensions[key] - now) / 1000);
        return res.status(403).json({ error: `You must wait ${secs} more second(s) before chatting again.` });
    }

    if (!gameChatActivity[key]) gameChatActivity[key] = [];
    gameChatActivity[key] = gameChatActivity[key].filter(t => now - t < 5000);
    gameChatActivity[key].push(now);

    // More than 4 messages in 5 seconds = 12 second cooldown
    if (gameChatActivity[key].length > 4) {
        gameChatSuspensions[key] = now + 12000;
        const secs = Math.ceil((gameChatSuspensions[key] - now) / 1000);
        return res.status(403).json({ error: `You are chatting too fast. Wait ${secs} second(s).` });
    }

    if (!gameChats[gameId]) gameChats[gameId] = [];

    const activePlayer = activePlayers[gameId] && activePlayers[gameId][req.userId];

    const newMsg = {
        id: crypto.randomUUID(),
        userId: req.userId,
        username: user ? user.username : 'Unknown',
        text,
        timestamp: now,
        sceneId: activePlayer ? activePlayer.sceneId : null,
        position: activePlayer ? {
            x: activePlayer.x,
            y: activePlayer.y,
            z: activePlayer.z
        } : null
    };

    gameChats[gameId].push(newMsg);
    if (gameChats[gameId].length > 100) gameChats[gameId].shift();

    res.json({ success: true, message: newMsg });
});

app.post('/api/games/:id/play-sync', requireAuth, (req, res) => {
    const gameId = req.params.id;
    const { x, y, z, rotY, sceneId, color } = req.body;
    const user = db.users.find(u => u.id === req.userId);

if (!activePlayers[gameId]) activePlayers[gameId] = {};

const lastChatMessage = (gameChats[gameId] || [])
    .slice()
    .reverse()
    .find(m => m.userId === req.userId && (Date.now() - m.timestamp) < 7000);

activePlayers[gameId][req.userId] = { 
    x, y, z, rotY, sceneId, username: user.username, 
    color: color || user.color || '#e74c3c', 
    equipped: user.equipped,
    timestamp: Date.now(),
    activeChatBubble: lastChatMessage ? {
        text: lastChatMessage.text,
        timestamp: lastChatMessage.timestamp
    } : null
};

const others = [];
for (let uId in activePlayers[gameId]) {
    // Check if their last ping was within 3 seconds
    if (Date.now() - activePlayers[gameId][uId].timestamp < 3000) {
        if (uId !== req.userId && activePlayers[gameId][uId].sceneId === sceneId) {
            others.push({ userId: uId, ...activePlayers[gameId][uId] });
        }
    } else {
        // Remove timed-out player
        delete activePlayers[gameId][uId];
    }
}

// If nobody is left in this game server, wipe its temporary server chat too
if (!activePlayers[gameId] || Object.keys(activePlayers[gameId]).length === 0) {
    delete activePlayers[gameId];
    delete gameChats[gameId];

    // Clean any per-user chat cooldown/spam data for this game too
    for (const key in gameChatActivity) {
        if (key.startsWith(gameId + '_')) delete gameChatActivity[key];
    }
    for (const key in gameChatSuspensions) {
        if (key.startsWith(gameId + '_')) delete gameChatSuspensions[key];
    }
}

res.json(others);
});



app.get('/api/games/:id/server-players', requireAuth, (req, res) => {
    const gameId = req.params.id;
    const now = Date.now();

    if (!activePlayers[gameId]) {
        return res.json({ players: [] });
    }

    const players = [];

    for (const userId in activePlayers[gameId]) {
        const p = activePlayers[gameId][userId];

        if (!p || now - p.timestamp > 3000) {
            delete activePlayers[gameId][userId];
            continue;
        }

        players.push({
            userId,
            username: p.username || 'Unknown Player'
        });
    }

    players.sort((a, b) => a.username.localeCompare(b.username));

    res.json({ players });
});

app.get('/sitemap.xml', (req, res) => {
  res.sendFile(__dirname + '/sitemap.xml');
});

// ==========================================
// SEO PAGE ROUTING
// ==========================================
// Serve pre-rendered SEO pages (e.g., visiting /seo/games/123 serves public/seo/games/123.html)
app.get('/seo/*any', (req, res, next) => {
    // Strip '/seo' from the path to get the relative filename
    const relativePath = req.path.replace('/seo', '');
    let filePath = path.join(__dirname, 'public', 'seo', relativePath);

    // If the path doesn't end in .html, append it so the file system can find it
    if (!filePath.endsWith('.html')) {
        filePath += '.html';
    }

    // Serve the file if it exists; otherwise, pass the request to the SPA catch-all
    if (fs.existsSync(filePath)) {
        res.sendFile(filePath);
    } else {
        next(); 
    }
});

// Your existing SPA catch-all
app.get('*any', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});
app.listen(PORT, () => {
    console.log(`Playsculpt server running on http://localhost:${PORT}`);
});
