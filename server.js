const express = require('express');
const crypto = require('crypto');
const path = require('path');
const fs = require('fs');

const app = express();
const PORT = 3000;

app.use(express.json({ limit: '10mb' })); 
app.use(express.static(path.join(__dirname, 'public')));

// In-memory databases
const DB_FILE = path.join(__dirname, 'db.json');
let db = {
    users: [], 
    sessions: {}, 
    games: [], 
    shopItems: [],
    groups: [],
    privateServers: [],
    activities: [] // Global activity feed
};

let activeEditors = {}; 
let activePlayers = {}; 
let onlineUsers = {};   

// Load existing DB if available & migrate data
if (fs.existsSync(DB_FILE)) {
    try {
        const loaded = JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
        db = { ...db, ...loaded };
        
        if (!db.shopItems) db.shopItems = [];
        if (!db.groups) db.groups = [];
        if (!db.privateServers) db.privateServers = [];
        if (!db.activities) db.activities = [];

        db.users.forEach(u => { 
            if (!u.followers) u.followers = []; 
            if (!u.friends) u.friends = [];
            if (!u.friendRequests) u.friendRequests = [];
            if (!u.color) u.color = '#e74c3c';
            if (!u.recentlyPlayed) u.recentlyPlayed = [];
            if (!u.badges) u.badges = [];
            if (!u.messages) u.messages = [];
            if (!u.inventory) u.inventory = [];
            if (!u.bookmarks) u.bookmarks = []; 
            if (typeof u.equipped === 'undefined') u.equipped = null;
            if (typeof u.primaryGroupId === 'undefined') u.primaryGroupId = null; 
            if (typeof u.coins === 'undefined') u.coins = 0; 
            if (typeof u.boosts === 'undefined') u.boosts = 0;
            if (typeof u.loginStreak === 'undefined') u.loginStreak = 0;
            if (typeof u.lastLoginReward === 'undefined') u.lastLoginReward = 0;
            if (!u.status) u.status = '';
            if (!u.profileTheme) u.profileTheme = '#ffffff';
            if (!u.loginHistory) u.loginHistory = [];
            
            if (u.friends.length > 0 && typeof u.friends[0] === 'string') {
                u.friends = u.friends.map(id => ({ id, addedAt: Date.now() }));
            }
        });
        db.games.forEach(g => { 
            if (!g.collaborators) g.collaborators = []; 
            if (!g.lastEditTime) g.lastEditTime = 0;
            if (!g.likes) g.likes = [];
            if (typeof g.plays !== 'number') g.plays = 0;
            if (!g.updates) g.updates = []; 
            if (!g.genre) g.genre = 'Sandbox'; 
            if (typeof g.groupId === 'undefined') g.groupId = null;
            if (typeof g.boosts === 'undefined') g.boosts = 0;
            if (!g.analytics) g.analytics = { plays: [], revenue: [], likes: [], sessions: [], uniquePlayers: {} };
            if (typeof g.maxPlayers === 'undefined') g.maxPlayers = 20;
            if (typeof g.psEnabled === 'undefined') g.psEnabled = false;
            if (typeof g.psPrice === 'undefined') g.psPrice = 0;
            if (!g.tags) g.tags = [];
            if (!g.versions) g.versions = [];
            if (typeof g.analytics.peakConcurrent === 'undefined') g.analytics.peakConcurrent = 0;
        });
        db.groups.forEach(gr => {
            if (typeof gr.level === 'undefined') gr.level = 1;
            if (typeof gr.xp === 'undefined') gr.xp = 0;
            if (!gr.events) gr.events = [];
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
            }
        });
        db.privateServers.forEach(ps => {
            if (typeof ps.joinEnabled === 'undefined') ps.joinEnabled = true;
            if (typeof ps.friendsOnly === 'undefined') ps.friendsOnly = false;
            if (!ps.whitelist) ps.whitelist = [];
            if (!ps.joinToken) ps.joinToken = null;
            if (!ps.tokenExpires) ps.tokenExpires = 0;
        });
    } catch (e) {
        console.error("Error loading db.json, starting fresh.");
    }
}

const saveDB = () => {
    fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2));
};

// --- Helpers ---
const hashPassword = (password) => {
    const salt = crypto.randomBytes(16).toString('hex');
    const hash = crypto.pbkdf2Sync(password, salt, 1000, 64, 'sha512').toString('hex');
    return { salt, hash };
};

const verifyPassword = (password, salt, hash) => {
    const verifyHash = crypto.pbkdf2Sync(password, salt, 1000, 64, 'sha512').toString('hex');
    return hash === verifyHash;
};

const requireAuth = (req, res, next) => {
    const token = req.headers.authorization;
    if (!token || !db.sessions[token]) {
        return res.status(401).json({ error: 'Unauthorized. Please log in.' });
    }
    req.userId = db.sessions[token];
    onlineUsers[req.userId] = Date.now(); 
    next();
};

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
    group.xp = (group.xp || 0) + amount;
    group.level = Math.floor(group.xp / 200) + 1;
};

const logActivity = (text) => {
    db.activities.unshift({ id: crypto.randomUUID(), text, timestamp: Date.now() });
    if (db.activities.length > 50) db.activities.pop();
};

// Periodic Session Cleanup
setInterval(() => {
    const now = Date.now();
    for (let gameId in activePlayers) {
        let gameInstanceCount = 0;
        for (let uId in activePlayers[gameId]) {
            if (now - activePlayers[gameId][uId].timestamp > 5000) {
                const joinTime = activePlayers[gameId][uId].joinTime;
                if (joinTime) {
                    const sessionLength = now - joinTime;
                    const game = db.games.find(g => g.id === gameId);
                    if (game && game.analytics) {
                        game.analytics.sessions.push({ t: now, duration: sessionLength });
                    }
                }
                delete activePlayers[gameId][uId];
            } else {
                gameInstanceCount++;
            }
        }
        const game = db.games.find(g => g.id === gameId);
        if (game && game.analytics && gameInstanceCount > game.analytics.peakConcurrent) {
            game.analytics.peakConcurrent = gameInstanceCount;
        }
    }
}, 5000);

// --- Global Activity Route ---
app.get('/api/activities', (req, res) => {
    res.json(db.activities);
});

// --- Routes ---
app.post('/api/signup', (req, res) => {
    const { username, password } = req.body;
    if (!username || !password || username.length < 3 || password.length < 5) {
        return res.status(400).json({ error: 'Invalid username or password length.' });
    }
    if (db.users.find(u => u.username.toLowerCase() === username.toLowerCase())) {
        return res.status(400).json({ error: 'Username already exists.' });
    }

    const { salt, hash } = hashPassword(password);
    const newUser = {
        id: crypto.randomUUID(), username, salt, hash,
        followers: [], friends: [], friendRequests: [],
        color: '#e74c3c', recentlyPlayed: [], badges: [], messages: [],
        inventory: [], bookmarks: [], equipped: null, primaryGroupId: null, 
        coins: 0, boosts: 0, loginStreak: 0, lastLoginReward: 0,
        status: '', profileTheme: '#ffffff', loginHistory: []
    };
    
    newUser.loginHistory.unshift({ time: Date.now(), userAgent: req.headers['user-agent'] || 'Unknown Device' });
    db.users.push(newUser);
    
    const token = crypto.randomBytes(32).toString('hex');
    db.sessions[token] = newUser.id;
    onlineUsers[newUser.id] = Date.now();
    saveDB();
    res.json({ token, username: newUser.username, userId: newUser.id, color: newUser.color, equipped: newUser.equipped, coins: newUser.coins, boosts: newUser.boosts });
});

app.post('/api/login', (req, res) => {
    const { username, password } = req.body;
    const user = db.users.find(u => u.username.toLowerCase() === username.toLowerCase());
    
    if (!user || !verifyPassword(password, user.salt, user.hash)) {
        return res.status(401).json({ error: 'Invalid username or password.' });
    }

    user.loginHistory.unshift({ time: Date.now(), userAgent: req.headers['user-agent'] || 'Unknown Device' });
    if (user.loginHistory.length > 5) user.loginHistory.pop();

    const token = crypto.randomBytes(32).toString('hex');
    db.sessions[token] = user.id;
    onlineUsers[user.id] = Date.now();
    saveDB();

    res.json({ token, username: user.username, userId: user.id, color: user.color, equipped: user.equipped, coins: user.coins, boosts: user.boosts });
});

app.get('/api/restore', requireAuth, (req, res) => {
    const user = db.users.find(u => u.id === req.userId);
    if(!user) return res.status(404).json({ error: "User not found" });
    res.json({ token: req.headers.authorization, username: user.username, userId: user.id, color: user.color, equipped: user.equipped, coins: user.coins, boosts: user.boosts });
});

app.post('/api/logout', requireAuth, (req, res) => {
    delete onlineUsers[req.userId];
    delete db.sessions[req.headers.authorization];
    saveDB();
    res.json({ message: 'Logged out successfully.' });
});

app.post('/api/me/daily-reward', requireAuth, (req, res) => {
    const user = db.users.find(u => u.id === req.userId);
    const now = Date.now();
    const ONE_DAY = 86400000;
    const TWO_DAYS = ONE_DAY * 2;
    const diff = now - (user.lastLoginReward || 0);

    if (diff >= ONE_DAY) {
        if (diff > TWO_DAYS) user.loginStreak = 1; 
        else user.loginStreak++;

        const earnedBoosts = Math.floor(Math.random() * 2) + 1; 
        let earnedCoins = 50;
        let milestone = false;

        if (user.loginStreak > 0 && user.loginStreak % 7 === 0) {
            earnedCoins += 300; milestone = true;
        }

        user.boosts += earnedBoosts;
        user.coins += earnedCoins;
        user.lastLoginReward = now;
        saveDB();
        return res.json({ rewarded: true, streak: user.loginStreak, coins: earnedCoins, boosts: earnedBoosts, totalCoins: user.coins, totalBoosts: user.boosts, milestone });
    }
    return res.json({ rewarded: false });
});

app.put('/api/me/settings', requireAuth, (req, res) => {
    const { newUsername, newPassword } = req.body;
    const user = db.users.find(u => u.id === req.userId);

    if (newUsername && newUsername !== user.username) {
        if (newUsername.length < 3) return res.status(400).json({ error: 'Username too short.' });
        if (db.users.find(u => u.username.toLowerCase() === newUsername.toLowerCase())) return res.status(400).json({ error: 'Username taken.' });
        user.username = newUsername;
        db.games.forEach(g => { if (g.authorId === user.id && !g.groupId) g.authorName = newUsername; });
        db.shopItems.forEach(i => { if (i.authorId === user.id) i.authorName = newUsername; });
    }

    if (newPassword) {
        if (newPassword.length < 5) return res.status(400).json({ error: 'Password too short.' });
        const { salt, hash } = hashPassword(newPassword);
        user.salt = salt; user.hash = hash;
    }
    saveDB();
    res.json({ message: 'Settings updated successfully!', username: user.username });
});

app.put('/api/me/status', requireAuth, (req, res) => {
    const user = db.users.find(u => u.id === req.userId);
    user.status = (req.body.status || '').substring(0, 100);
    saveDB();
    res.json({ success: true, status: user.status });
});

app.put('/api/me/theme', requireAuth, (req, res) => {
    const user = db.users.find(u => u.id === req.userId);
    if (user.coins < 25) return res.status(400).json({ error: 'Insufficient SculptCoins.' });
    user.coins -= 25;
    user.profileTheme = req.body.theme || '#ffffff';
    saveDB();
    res.json({ success: true, coins: user.coins, theme: user.profileTheme });
});

app.put('/api/me/primary-group', requireAuth, (req, res) => {
    const { groupId } = req.body;
    const user = db.users.find(u => u.id === req.userId);
    user.primaryGroupId = groupId || null;
    saveDB();
    res.json({ success: true });
});

app.get('/api/me/login-history', requireAuth, (req, res) => {
    const user = db.users.find(u => u.id === req.userId);
    res.json(user.loginHistory || []);
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

    if (!sender.friends.find(f => f.id === targetUser.id)) return res.status(403).json({ error: 'You can only message friends.' });

    if (!targetUser.messages) targetUser.messages = [];
    targetUser.messages.push({
        id: crypto.randomUUID(), fromId: sender.id, fromUsername: sender.username,
        text: text.trim().substring(0, 500), timestamp: Date.now()
    });
    saveDB();
    res.json({ message: 'Message sent!' });
});

app.get('/api/users/search', (req, res) => {
    const query = (req.query.q || '').toLowerCase();
    if (!query) return res.json([]);
    const results = db.users.filter(u => u.username.toLowerCase().includes(query))
        .map(u => ({ username: u.username, isOnline: isUserOnline(u.id) })).slice(0, 20);
    res.json(results);
});

app.get('/api/me', requireAuth, (req, res) => {
    const user = db.users.find(u => u.id === req.userId);
    const requests = user.friendRequests.map(id => { const u = db.users.find(usr => usr.id === id); return u ? { id: u.id, username: u.username } : null; }).filter(Boolean);
    const friendsList = user.friends.map(f => { const u = db.users.find(usr => usr.id === f.id); return u ? { id: u.id, username: u.username, addedAt: f.addedAt, isOnline: isUserOnline(u.id) } : null; }).filter(Boolean);
    const recentGames = user.recentlyPlayed.map(rp => { const g = db.games.find(gm => gm.id === rp.gameId); return g ? { id: g.id, title: g.title, authorName: g.authorName, tags: g.tags, genre: g.genre, likes: g.likes.length, plays: g.plays, timestamp: rp.timestamp } : null; }).filter(Boolean);
    const bookmarkedGames = (user.bookmarks || []).map(gameId => { const g = db.games.find(gm => gm.id === gameId); return g ? { id: g.id, title: g.title, authorName: g.authorName, tags: g.tags, genre: g.genre, likes: g.likes.length, plays: g.plays } : null; }).filter(Boolean);
    const myGroups = db.groups.filter(gr => gr.members.some(m => m.userId === user.id)).map(gr => {
        const mem = gr.members.find(m=>m.userId === user.id);
        const role = gr.roles.find(r => r.id === mem.roleId);
        return { id: gr.id, name: gr.name, roleName: role ? role.name : 'Member', perms: role ? role.perms : {} };
    });

    res.json({
        id: user.id, username: user.username, color: user.color, badges: user.badges, coins: user.coins, boosts: user.boosts,
        requests, friends: friendsList, recentlyPlayed: recentGames, bookmarkedGames, 
        unreadMessages: (user.messages || []).length, equipped: user.equipped, myGroups,
        status: user.status, profileTheme: user.profileTheme
    });
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
    const inventoryItems = user.inventory.map(itemId => db.shopItems.find(i => i.id === itemId)).filter(Boolean);
    const userGroups = db.groups.filter(gr => gr.members.some(m => m.userId === user.id)).map(gr => {
        const mem = gr.members.find(m=>m.userId === user.id);
        const role = gr.roles.find(r => r.id === mem.roleId);
        return { id: gr.id, name: gr.name, roleName: role ? role.name : 'Member', isPrimary: user.primaryGroupId === gr.id };
    });
    let primaryGroup = userGroups.find(g => g.isPrimary) || null;

    res.json({
        id: user.id, username: user.username, isOnline: isUserOnline(user.id), color: user.color, badges: user.badges,
        followersCount: user.followers.length, isFollowing, friendStatus, friends: friendsDetails,
        gamesCreated: userGames.length, status: user.status, profileTheme: user.profileTheme,
        games: userGames.map(g => ({ id: g.id, title: g.title, authorName: g.authorName, tags: g.tags, genre: g.genre, likes: g.likes.length, plays: g.plays, groupId: g.groupId })),
        likedGames: likedGames.map(g => ({ id: g.id, title: g.title, authorName: g.authorName, tags: g.tags, genre: g.genre, likes: g.likes.length, plays: g.plays, groupId: g.groupId })),
        inventory: inventoryItems, equipped: user.equipped, groups: userGroups, primaryGroup
    });
});

app.post('/api/users/:username/friend-request', requireAuth, (req, res) => {
    const targetUser = db.users.find(u => u.username.toLowerCase() === req.params.username.toLowerCase());
    if (!targetUser || targetUser.id === req.userId) return res.status(400).json({ error: 'Invalid user.' });
    if (!targetUser.friends.find(f => f.id === req.userId) && !targetUser.friendRequests.includes(req.userId)) {
        targetUser.friendRequests.push(req.userId);
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

// --- Groups Routes ---
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

app.get('/api/groups', (req, res) => {
    res.json(db.groups.map(gr => ({ id: gr.id, name: gr.name, description: gr.description, members: gr.members.length, coins: gr.coins })).sort((a,b) => b.members - a.members));
});

app.post('/api/groups', requireAuth, (req, res) => {
    const { name, description } = req.body;
    if (!name || name.trim().length < 3) return res.status(400).json({ error: 'Group name too short.' });
    if (db.groups.find(gr => gr.name.toLowerCase() === name.toLowerCase())) return res.status(400).json({ error: 'Group name already taken.' });

    const rOwnerId = crypto.randomUUID();
    const rMemberId = crypto.randomUUID();
    const newGroup = {
        id: crypto.randomUUID(), name: name.trim(), description: description || '', ownerId: req.userId,
        roles: [
            { id: rOwnerId, name: 'Owner', rank: 255, perms: { manageRanks: true, kick: true, ban: true, editGames: true, deletePosts: true, manageCategories: true, manageEvents: true, managePayouts: true } },
            { id: rMemberId, name: 'Member', rank: 1, perms: { manageRanks: false, kick: false, ban: false, editGames: false, deletePosts: false, manageCategories: false, manageEvents: false, managePayouts: false } }
        ],
        members: [{ userId: req.userId, roleId: rOwnerId, joinedAt: Date.now() }],
        posts: [], categories: [], threads: [], banned: [], events: [], coins: 0, level: 1, xp: 0, createdAt: Date.now()
    };
    db.groups.push(newGroup); saveDB();
    res.json({ message: 'Group created!', groupId: newGroup.id });
});

app.get('/api/groups/:id', (req, res) => {
    const group = db.groups.find(gr => gr.id === req.params.id);
    if (!group) return res.status(404).json({ error: 'Group not found.' });

    const memberDetails = group.members.map(m => {
        const u = db.users.find(usr => usr.id === m.userId);
        const role = group.roles.find(r => r.id === m.roleId);
        return u ? { userId: u.id, username: u.username, roleName: role ? role.name : 'Unknown', rank: role ? role.rank : 0, isOnline: isUserOnline(u.id) } : null;
    }).filter(Boolean).sort((a,b) => b.rank - a.rank);

    const groupGames = db.games.filter(g => g.groupId === group.id).map(g => ({
        id: g.id, title: g.title, authorName: g.authorName, tags: g.tags, genre: g.genre, likes: g.likes.length, plays: g.plays, groupId: g.groupId
    }));

    let myPerms = null, myRank = -1;
    if (req.headers.authorization && db.sessions[req.headers.authorization]) {
        const userId = db.sessions[req.headers.authorization];
        myPerms = getGroupMemberPerms(group, userId);
        myRank = getGroupMemberRank(group, userId);
    }

    res.json({
        id: group.id, name: group.name, description: group.description, coins: group.coins, level: group.level, xp: group.xp,
        posts: group.posts.slice(0, 50), members: memberDetails, games: groupGames, events: group.events || [],
        roles: group.roles, categories: group.categories, myPerms, myRank
    });
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
    if (!group.members.find(m => m.userId === req.userId)) return res.status(403).json({ error: 'Must be a member to post.' });

    const user = db.users.find(u => u.id === req.userId);
    group.posts.unshift({
        id: crypto.randomUUID(), authorName: user.username, authorId: user.id, text: text.trim().substring(0, 200), timestamp: Date.now()
    });

    addGroupXp(group, 5); 
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
    addGroupXp(group, 5); 
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
    addGroupXp(group, 5); 
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

// --- Game Routes & Analytics ---

app.post('/api/games', requireAuth, (req, res) => {
    const { title, gameData, genre, groupId, tags, maxPlayers, psEnabled, psPrice } = req.body;
    if (!title || !gameData) return res.status(400).json({ error: 'Missing game data.' });
    
    const user = db.users.find(u => u.id === req.userId);
    let authorName = user.username;

    if (groupId) {
        const group = db.groups.find(gr => gr.id === groupId);
        const perms = getGroupMemberPerms(group, req.userId);
        if (!group || !perms || !perms.editGames) return res.status(403).json({ error: 'Not authorized to publish to this group.' });
        authorName = group.name; 
    }

    const tArr = (tags || '').split(',').map(t=>t.trim().toLowerCase()).filter(t=>t.length>0).slice(0, 10);

    const newGame = {
        id: crypto.randomUUID(), title, authorId: user.id, authorName: authorName, genre: genre || 'Sandbox', tags: tArr,
        maxPlayers: parseInt(maxPlayers) || 20, psEnabled: !!psEnabled, psPrice: parseInt(psPrice) || 0,
        groupId: groupId || null, boosts: 0, analytics: { plays: [], revenue: [], likes: [], sessions: [], uniquePlayers: {} },
        gameData, versions: [], lastEditTime: Date.now(), collaborators: [], likes: [], plays: 0, updates: [], createdAt: new Date().toISOString()
    };
    db.games.push(newGame);
    awardBadge(req.userId, 'Creator');
    logActivity(`${user.username} published a new game: ${title}`);
    saveDB();
    res.json({ message: 'Game saved successfully!', gameId: newGame.id });
});

app.get('/api/games', (req, res) => {
    const query = (req.query.q || '').toLowerCase();
    const genre = req.query.genre || 'All';
    const tag = (req.query.tag || '').toLowerCase();
    
    const publicGames = db.games
        .filter(g => query === '' || g.title.toLowerCase().includes(query))
        .filter(g => genre === 'All' || g.genre === genre)
        .filter(g => tag === '' || (g.tags && g.tags.includes(tag)))
        .map(g => ({
            id: g.id, title: g.title, authorName: g.authorName, tags: g.tags, genre: g.genre, likes: g.likes.length, plays: g.plays, groupId: g.groupId, boosts: g.boosts || 0, createdAt: g.createdAt
        })).sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
    res.json(publicGames);
});

app.get('/api/games/launchpad', (req, res) => {
    const launchpadGames = db.games.filter(g => g.boosts > 0).sort((a, b) => b.boosts - a.boosts).slice(0, 10)
        .map(g => ({ id: g.id, title: g.title, authorName: g.authorName, tags: g.tags, genre: g.genre, likes: g.likes.length, plays: g.plays, groupId: g.groupId, boosts: g.boosts }));
    res.json(launchpadGames);
});

app.post('/api/games/:id/boost', requireAuth, (req, res) => {
    const user = db.users.find(u => u.id === req.userId);
    const game = db.games.find(g => g.id === req.params.id);
    if (!game) return res.status(404).json({ error: 'Game not found.' });
    if (user.boosts < 5) return res.status(400).json({ error: 'You need at least 5 Boosts to boost a game.' });

    user.boosts -= 5;
    game.boosts = (game.boosts || 0) + 1;
    saveDB();
    res.json({ message: 'Game Boosted successfully!', boosts: user.boosts });
});

app.get('/api/games/trending', (req, res) => {
    res.json([...db.games].sort((a, b) => (b.plays + b.likes.length * 2) - (a.plays + a.likes.length * 2)).slice(0, 4).map(g => ({ id: g.id, title: g.title, authorName: g.authorName, tags: g.tags, genre: g.genre, likes: g.likes.length, plays: g.plays, groupId: g.groupId })));
});

app.get('/api/games/most-liked', (req, res) => {
    res.json([...db.games].sort((a, b) => b.likes.length - a.likes.length).slice(0, 4).map(g => ({ id: g.id, title: g.title, authorName: g.authorName, tags: g.tags, genre: g.genre, likes: g.likes.length, plays: g.plays, groupId: g.groupId })));
});

app.get('/api/games/fresh', (req, res) => {
    res.json([...db.games].sort((a, b) => {
            const tA = a.lastEditTime || new Date(a.createdAt).getTime();
            const tB = b.lastEditTime || new Date(b.createdAt).getTime();
            return tB - tA;
        }).slice(0, 4).map(g => ({ id: g.id, title: g.title, authorName: g.authorName, tags: g.tags, genre: g.genre, likes: g.likes.length, plays: g.plays, groupId: g.groupId })));
});

app.get('/api/my-games', requireAuth, (req, res) => {
    const userGroups = db.groups.filter(gr => { const perms = getGroupMemberPerms(gr, req.userId); return perms && perms.editGames; });
    const groupIds = userGroups.map(gr => gr.id);
    const myGames = db.games.filter(g => g.authorId === req.userId || g.collaborators.includes(req.userId) || (g.groupId && groupIds.includes(g.groupId)))
        .map(g => ({ id: g.id, title: g.title, authorName: g.authorName, tags: g.tags, genre: g.genre, likes: g.likes.length, plays: g.plays, groupId: g.groupId }));
    res.json(myGames);
});

// Private Server Management Endpoints
app.get('/api/private-servers/:id', requireAuth, (req, res) => {
    const ps = db.privateServers.find(p => p.id === req.params.id);
    if (!ps) return res.status(404).json({error: 'Not found'});
    if (ps.ownerId !== req.userId) return res.status(403).json({error: 'Not authorized'});
    
    const whitelistUsers = ps.whitelist.map(id => {
        const u = db.users.find(usr => usr.id === id);
        return u ? { id: u.id, username: u.username } : null;
    }).filter(Boolean);
    
    res.json({ ...ps, whitelistUsers });
});

app.put('/api/private-servers/:id/settings', requireAuth, (req, res) => {
    const ps = db.privateServers.find(p => p.id === req.params.id);
    if (!ps || ps.ownerId !== req.userId) return res.status(403).json({error: 'Not authorized'});
    ps.joinEnabled = req.body.joinEnabled;
    ps.friendsOnly = req.body.friendsOnly;
    saveDB();
    res.json({ success: true });
});

app.post('/api/private-servers/:id/whitelist', requireAuth, (req, res) => {
    const ps = db.privateServers.find(p => p.id === req.params.id);
    if (!ps || ps.ownerId !== req.userId) return res.status(403).json({error: 'Not authorized'});
    const targetUser = db.users.find(u => u.username.toLowerCase() === req.body.username.toLowerCase());
    if (!targetUser) return res.status(404).json({error: 'User not found'});
    
    if (!ps.whitelist.includes(targetUser.id)) {
        ps.whitelist.push(targetUser.id);
        saveDB();
    }
    res.json({ success: true });
});

app.delete('/api/private-servers/:id/whitelist/:userId', requireAuth, (req, res) => {
    const ps = db.privateServers.find(p => p.id === req.params.id);
    if (!ps || ps.ownerId !== req.userId) return res.status(403).json({error: 'Not authorized'});
    ps.whitelist = ps.whitelist.filter(id => id !== req.params.userId);
    saveDB();
    res.json({ success: true });
});

app.post('/api/private-servers/:id/generate-link', requireAuth, (req, res) => {
    const ps = db.privateServers.find(p => p.id === req.params.id);
    if (!ps || ps.ownerId !== req.userId) return res.status(403).json({error: 'Not authorized'});
    ps.joinToken = crypto.randomUUID();
    ps.tokenExpires = Date.now() + (3 * 60 * 60 * 1000); // 3 hours
    saveDB();
    res.json({ token: ps.joinToken });
});

app.post('/api/private-servers/join-link', requireAuth, (req, res) => {
    const { token } = req.body;
    const ps = db.privateServers.find(p => p.joinToken === token);
    if (!ps || Date.now() > ps.tokenExpires) return res.status(400).json({error: 'Link invalid or expired'});
    if (!ps.joinEnabled) return res.status(400).json({error: 'Joining is currently disabled by the owner.'});
    
    if (!ps.whitelist.includes(req.userId)) {
        ps.whitelist.push(req.userId);
        saveDB();
    }
    res.json({ gameId: ps.gameId, serverId: ps.id });
});

app.get('/api/games/:id', (req, res) => {
    const game = db.games.find(g => g.id === req.params.id);
    if (!game) return res.status(404).json({ error: 'Game not found.' });
    
    let isLiked = false, isBookmarked = false;
    const token = req.headers.authorization;
    if (token && db.sessions[token]) {
        const userId = db.sessions[token];
        if (game.likes.includes(userId)) isLiked = true;
        const user = db.users.find(u => u.id === userId);
        if (user && user.bookmarks.includes(game.id)) isBookmarked = true;
    }
    
    const servers = { public: [], private: [] };
    if (activePlayers[game.id]) {
        const instCounts = {};
        for(let uid in activePlayers[game.id]) {
            const iId = activePlayers[game.id][uid].instanceId;
            instCounts[iId] = (instCounts[iId] || 0) + 1;
        }
        for(let iId in instCounts) {
            if(!iId.startsWith('priv_')) {
                servers.public.push({ id: iId, players: instCounts[iId], max: game.maxPlayers || 20 });
            }
        }
    }
    
    let myPrivateServer = null;
    if (token && db.sessions[token]) {
        myPrivateServer = db.privateServers.find(ps => ps.gameId === game.id && ps.ownerId === db.sessions[token]);
    }

    res.json({ 
        ...game, likesCount: game.likes.length, isLiked, isBookmarked, updates: game.updates || [],
        activeServers: servers, myPrivateServer, versions: game.versions.map(v => ({ id: v.id, timestamp: v.timestamp }))
    });
});

app.post('/api/games/:id/bookmark', requireAuth, (req, res) => {
    const user = db.users.find(u => u.id === req.userId);
    const gameId = req.params.id;
    if (!db.games.find(g => g.id === gameId)) return res.status(404).json({ error: 'Game not found.' });

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

app.post('/api/games/:id/buy-ps', requireAuth, (req, res) => {
    const game = db.games.find(g => g.id === req.params.id);
    if (!game || !game.psEnabled) return res.status(400).json({ error: 'Private servers not enabled.' });
    const user = db.users.find(u => u.id === req.userId);
    if (user.coins < game.psPrice) return res.status(400).json({ error: 'Insufficient funds.' });
    
    if (db.privateServers.find(ps => ps.gameId === game.id && ps.ownerId === user.id)) return res.status(400).json({ error: 'You already own one.' });
    
    user.coins -= game.psPrice;
    if (game.psPrice > 0 && game.groupId) {
        const group = db.groups.find(gr => gr.id === game.groupId);
        if (group) group.coins += game.psPrice;
    }

    const ps = { id: crypto.randomUUID(), gameId: game.id, ownerId: user.id, whitelist: [], joinEnabled: true, friendsOnly: false, joinToken: null, tokenExpires: 0 };
    db.privateServers.push(ps);
    saveDB();
    res.json({ success: true, coins: user.coins, privateServer: ps });
});

app.get('/api/games/:id/analytics', requireAuth, (req, res) => {
    const game = db.games.find(g => g.id === req.params.id);
    if (!game) return res.status(404).json({ error: 'Game not found.' });

    let canView = game.authorId === req.userId || game.collaborators.includes(req.userId);
    if (game.groupId) {
        const group = db.groups.find(gr => gr.id === game.groupId);
        const perms = getGroupMemberPerms(group, req.userId);
        if (perms && perms.editGames) canView = true;
    }
    if (!canView) return res.status(403).json({ error: 'Not authorized.' });

    const timeframe = parseInt(req.query.timeframe) || 7; 
    const cutoff = Date.now() - (timeframe * 86400000);

    const filterEvts = (arr) => (arr || []).filter(e => e.t >= cutoff);
    const relevantPlays = filterEvts(game.analytics.plays);
    const relevantRev = filterEvts(game.analytics.revenue);
    const relevantLikes = filterEvts(game.analytics.likes);
    const relevantSessions = filterEvts(game.analytics.sessions);

    let totalRevenue = relevantRev.reduce((acc, curr) => acc + curr.amt, 0);

    const sourceCounts = {};
    const hourCounts = new Array(24).fill(0);
    relevantPlays.forEach(p => {
        sourceCounts[p.source] = (sourceCounts[p.source] || 0) + 1;
        const hr = new Date(p.t).getHours();
        hourCounts[hr]++;
    });

    let topSource = 'None', maxSrc = 0;
    for (let s in sourceCounts) { if (sourceCounts[s] > maxSrc) { topSource = s; maxSrc = sourceCounts[s]; } }

    let peakHour = 0, maxHr = 0;
    for (let i=0; i<24; i++) { if (hourCounts[i] > maxHr) { peakHour = i; maxHr = hourCounts[i]; } }
    let peakHourStr = maxHr === 0 ? 'N/A' : (peakHour === 0 ? '12 AM' : (peakHour < 12 ? `${peakHour} AM` : (peakHour === 12 ? '12 PM' : `${peakHour-12} PM`)));

    let totalPlayers = 0; let returnedPlayers = 0;
    for (let uId in game.analytics.uniquePlayers) {
        totalPlayers++;
        if (game.analytics.uniquePlayers[uId] > 1) returnedPlayers++;
    }
    const retention = totalPlayers === 0 ? 0 : Math.round((returnedPlayers / totalPlayers) * 100);

    let avgSession = 0;
    if (relevantSessions.length > 0) {
        const totalTime = relevantSessions.reduce((acc, s) => acc + s.duration, 0);
        avgSession = Math.round((totalTime / relevantSessions.length) / 60000); 
    }

    res.json({
        totalPlays: relevantPlays.length, totalRevenue, totalLikes: relevantLikes.length,
        topSource, peakHourStr, retention, avgSession, peakConcurrent: game.analytics.peakConcurrent || 0
    });
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

app.post('/api/games/:id/play', requireAuth, (req, res) => {
    const user = db.users.find(u => u.id === req.userId);
    const gameId = req.params.id;
    const { source } = req.body;
    const game = db.games.find(g => g.id === gameId);
    let revenueEarned = 5;

    if (game) {
        game.plays = (game.plays || 0) + 1;
        if (!game.analytics) game.analytics = { plays: [], revenue: [], likes: [], sessions: [], uniquePlayers: {} };
        game.analytics.plays.push({ t: Date.now(), source: source || 'unknown' });
        game.analytics.revenue.push({ t: Date.now(), amt: revenueEarned });
        game.analytics.uniquePlayers[user.id] = (game.analytics.uniquePlayers[user.id] || 0) + 1;

        if (game.groupId) {
            const group = db.groups.find(gr => gr.id === game.groupId);
            if (group) { group.coins = (group.coins || 0) + revenueEarned; addGroupXp(group, 10); }
        }
        
        logActivity(`${user.username} played ${game.title}`);
    }

    user.coins += 5; 
    user.recentlyPlayed = user.recentlyPlayed.filter(g => g.gameId !== gameId);
    user.recentlyPlayed.unshift({ gameId, timestamp: Date.now() });
    if (user.recentlyPlayed.length > 8) user.recentlyPlayed.pop();

    awardBadge(req.userId, 'Gamer');
    saveDB();
    res.json({ success: true, coins: user.coins });
});

app.post('/api/games/:id/like', requireAuth, (req, res) => {
    const game = db.games.find(g => g.id === req.params.id);
    const user = db.users.find(u => u.id === req.userId);
    if (!game) return res.status(404).json({ error: 'Game not found.' });

    let isLiked = false;
    if (!game.analytics) game.analytics = { plays: [], revenue: [], likes: [], sessions: [], uniquePlayers: {} };

    if (game.likes.includes(req.userId)) {
        game.likes = game.likes.filter(id => id !== req.userId);
    } else {
        game.likes.push(req.userId);
        game.analytics.likes.push({ t: Date.now() });
        isLiked = true;
        awardBadge(req.userId, 'Critic');
        logActivity(`${user.username} liked ${game.title}`);
    }
    saveDB();
    res.json({ likesCount: game.likes.length, isLiked });
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

    const { gameData, genre, tags, maxPlayers, psEnabled, psPrice, lastLocalEditTime } = req.body;
    if (!activeEditors[game.id]) activeEditors[game.id] = {};
    activeEditors[game.id][req.userId] = Date.now();

    const activeUsernames = [];
    for (let uId in activeEditors[game.id]) {
        if (Date.now() - activeEditors[game.id][uId] < 4000) {
            const u = db.users.find(usr => usr.id === uId);
            if (u) activeUsernames.push(u.username);
        } else delete activeEditors[game.id][uId];
    }

    let appliedUpdate = false;
    if (gameData && lastLocalEditTime > game.lastEditTime) {
        if (game.gameData && game.gameData.platforms && game.gameData.platforms.length > 0) {
            game.versions.unshift({ id: crypto.randomUUID(), timestamp: Date.now(), data: game.gameData });
            if (game.versions.length > 10) game.versions.pop(); 
        }

        game.gameData = gameData;
        if (genre) game.genre = genre;
        if (tags) game.tags = tags.split(',').map(t=>t.trim().toLowerCase()).filter(t=>t.length>0).slice(0, 10);
        if (maxPlayers) game.maxPlayers = Math.max(1, Math.min(500, parseInt(maxPlayers)));
        game.psEnabled = !!psEnabled;
        game.psPrice = parseInt(psPrice) || 0;
        
        game.lastEditTime = lastLocalEditTime;
        saveDB(); appliedUpdate = true;
    }

    res.json({ gameData: game.gameData, genre: game.genre, tags: game.tags, lastEditTime: game.lastEditTime, activeEditors: activeUsernames, acceptedLocalUpdate: appliedUpdate });
});

app.post('/api/games/:id/rollback', requireAuth, (req, res) => {
    const game = db.games.find(g => g.id === req.params.id);
    if (!game) return res.status(404).json({ error: 'Game not found.' });
    let canEdit = game.authorId === req.userId || game.collaborators.includes(req.userId);
    if (game.groupId) {
        const group = db.groups.find(gr => gr.id === game.groupId);
        const perms = getGroupMemberPerms(group, req.userId);
        if (perms && perms.editGames) canEdit = true;
    }
    if (!canEdit) return res.status(403).json({ error: 'Not authorized.' });

    const { versionId } = req.body;
    const version = game.versions.find(v => v.id === versionId);
    if (!version) return res.status(404).json({ error: 'Version not found.' });

    game.versions.unshift({ id: crypto.randomUUID(), timestamp: Date.now(), data: game.gameData });
    game.gameData = version.data;
    game.lastEditTime = Date.now();
    saveDB();
    res.json({ success: true, gameData: game.gameData });
});

app.post('/api/games/:id/play-sync', requireAuth, (req, res) => {
    const gameId = req.params.id;
    const { x, y, z, rotY, sceneId, color, requestedInstanceId } = req.body;
    const user = db.users.find(u => u.id === req.userId);
    const game = db.games.find(g => g.id === gameId);
    if (!game) return res.status(404).json({error: 'Game missing'});

    if (!activePlayers[gameId]) activePlayers[gameId] = {};
    
    let myInst = requestedInstanceId;
    if (!myInst) {
        const instCounts = {};
        for(let uid in activePlayers[gameId]) {
            const playerRec = activePlayers[gameId][uid];
            if(Date.now() - playerRec.timestamp < 5000 && !playerRec.instanceId.startsWith('priv_')) {
                instCounts[playerRec.instanceId] = (instCounts[playerRec.instanceId] || 0) + 1;
            }
        }
        const maxP = game.maxPlayers || 20;
        myInst = Object.keys(instCounts).find(iId => instCounts[iId] < maxP);
        if (!myInst) myInst = 'pub_' + crypto.randomUUID().substring(0,8);
    } else if (myInst.startsWith('priv_')) {
        // Enforce private server rules
        const ps = db.privateServers.find(p => p.id === myInst.replace('priv_',''));
        if (ps) {
            const isOwner = ps.ownerId === req.userId;
            const isFriend = user.friends.some(f => f.id === ps.ownerId);
            const isWhitelisted = ps.whitelist.includes(req.userId);
            
            let allowed = isOwner || isWhitelisted;
            if (!allowed && ps.joinEnabled) {
                if (ps.friendsOnly && isFriend) allowed = true;
                if (!ps.friendsOnly) allowed = true; // Wait, if it's not friends only and join is enabled, let anyone join if they have the link? Link usually whitelists them. So only allow if whitelisted.
            }
            if (!allowed) return res.status(403).json({ error: "Cannot join this private server." });
        }
    }

    let joinTime = Date.now();
    if (activePlayers[gameId][req.userId] && activePlayers[gameId][req.userId].instanceId === myInst) {
        joinTime = activePlayers[gameId][req.userId].joinTime || joinTime;
    }

    activePlayers[gameId][req.userId] = { 
        x, y, z, rotY, sceneId, username: user.username, 
        color: color || user.color || '#e74c3c', 
        equipped: user.equipped,
        instanceId: myInst, joinTime, timestamp: Date.now() 
    };

    const others = [];
    for (let uId in activePlayers[gameId]) {
        if (Date.now() - activePlayers[gameId][uId].timestamp < 3000) {
            if (uId !== req.userId && activePlayers[gameId][uId].instanceId === myInst && activePlayers[gameId][uId].sceneId === sceneId) {
                others.push(activePlayers[gameId][uId]);
            }
        }
    }
    res.json({ others, instanceId: myInst });
});

// JSON error catch-all for /api routes
app.use('/api', (req, res) => {
    res.status(404).json({ error: 'API endpoint not found' });
});

app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, () => {
    console.log(`Playsculpt server running on http://localhost:${PORT}`);
});