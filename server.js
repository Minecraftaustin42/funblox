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
    groups: [] 
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
            if (typeof u.coins === 'undefined') u.coins = 0; // Migrate coins to backend
            
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
        });

        // Migrate Groups to advanced roles system
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
        inventory: [], bookmarks: [], equipped: null, primaryGroupId: null, coins: 0
    };
    db.users.push(newUser);
    
    const token = crypto.randomBytes(32).toString('hex');
    db.sessions[token] = newUser.id;
    onlineUsers[newUser.id] = Date.now();
    saveDB();
    res.json({ token, username: newUser.username, userId: newUser.id, color: newUser.color, equipped: newUser.equipped, coins: newUser.coins });
});

app.post('/api/login', (req, res) => {
    const { username, password } = req.body;
    const user = db.users.find(u => u.username.toLowerCase() === username.toLowerCase());
    
    if (!user || !verifyPassword(password, user.salt, user.hash)) {
        return res.status(401).json({ error: 'Invalid username or password.' });
    }

    const token = crypto.randomBytes(32).toString('hex');
    db.sessions[token] = user.id;
    onlineUsers[user.id] = Date.now();
    saveDB();

    res.json({ token, username: user.username, userId: user.id, color: user.color, equipped: user.equipped, coins: user.coins });
});

app.get('/api/restore', requireAuth, (req, res) => {
    const user = db.users.find(u => u.id === req.userId);
    if(!user) return res.status(404).json({ error: "User not found" });
    res.json({ token: req.headers.authorization, username: user.username, userId: user.id, color: user.color, equipped: user.equipped, coins: user.coins });
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
    res.json({ message: 'Message sent!' });
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
        unreadMessages: (user.messages || []).length, equipped: user.equipped, myGroups
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
        followersCount: user.followers.length, isFollowing, friendStatus, friends: friendsDetails,
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

app.get('/api/groups', (req, res) => {
    const publicGroups = db.groups.map(gr => ({
        id: gr.id, name: gr.name, description: gr.description, members: gr.members.length, coins: gr.coins
    })).sort((a,b) => b.members - a.members);
    res.json(publicGroups);
});

app.post('/api/groups', requireAuth, (req, res) => {
    const { name, description } = req.body;
    if (!name || name.trim().length < 3) return res.status(400).json({ error: 'Group name too short.' });
    if (db.groups.find(gr => gr.name.toLowerCase() === name.toLowerCase())) {
        return res.status(400).json({ error: 'Group name already taken.' });
    }

    const rOwnerId = crypto.randomUUID();
    const rMemberId = crypto.randomUUID();
    const newGroup = {
        id: crypto.randomUUID(), name: name.trim(), description: description || '',
        ownerId: req.userId,
        roles: [
            { id: rOwnerId, name: 'Owner', rank: 255, perms: { manageRanks: true, kick: true, ban: true, editGames: true, deletePosts: true, manageCategories: true, manageEvents: true, managePayouts: true } },
            { id: rMemberId, name: 'Member', rank: 1, perms: { manageRanks: false, kick: false, ban: false, editGames: false, deletePosts: false, manageCategories: false, manageEvents: false, managePayouts: false } }
        ],
        members: [{ userId: req.userId, roleId: rOwnerId, joinedAt: Date.now() }],
        posts: [], categories: [], threads: [], banned: [], events: [], coins: 0, level: 1, xp: 0, createdAt: Date.now()
    };
    db.groups.push(newGroup);
    saveDB();
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
        id: g.id, title: g.title, authorName: g.authorName, genre: g.genre, likes: g.likes.length, plays: g.plays, groupId: g.groupId
    }));

    let myPerms = null, myRank = -1;
    if (req.headers.authorization && db.sessions[req.headers.authorization]) {
        const userId = db.sessions[req.headers.authorization];
        myPerms = getGroupMemberPerms(group, userId);
        myRank = getGroupMemberRank(group, userId);
    }

    res.json({
        id: group.id, name: group.name, description: group.description, coins: group.coins,
        level: group.level, xp: group.xp,
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

app.post('/api/games', requireAuth, (req, res) => {
    const { title, gameData, genre, groupId } = req.body;
    if (!title || !gameData) return res.status(400).json({ error: 'Missing game data.' });
    
    const user = db.users.find(u => u.id === req.userId);
    let authorName = user.username;

    if (groupId) {
        const group = db.groups.find(gr => gr.id === groupId);
        const perms = getGroupMemberPerms(group, req.userId);
        if (!group || !perms || !perms.editGames) {
            return res.status(403).json({ error: 'Not authorized to publish to this group.' });
        }
        authorName = group.name; 
    }

    const newGame = {
        id: crypto.randomUUID(), title, authorId: user.id, authorName: authorName, genre: genre || 'Sandbox',
        groupId: groupId || null,
        gameData, lastEditTime: Date.now(), collaborators: [], likes: [], plays: 0, updates: [], createdAt: new Date().toISOString()
    };
    db.games.push(newGame);
    awardBadge(req.userId, 'Creator');
    saveDB();
    res.json({ message: 'Game saved successfully!', gameId: newGame.id });
});

app.get('/api/games', (req, res) => {
    const query = (req.query.q || '').toLowerCase();
    const genre = req.query.genre || 'All';
    const publicGames = db.games
        .filter(g => query === '' || g.title.toLowerCase().includes(query))
        .filter(g => genre === 'All' || g.genre === genre)
        .map(g => ({
            id: g.id, title: g.title, authorName: g.authorName, genre: g.genre, likes: g.likes.length, plays: g.plays, groupId: g.groupId, createdAt: g.createdAt
        })).sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
    res.json(publicGames);
});

app.get('/api/games/trending', (req, res) => {
    const trending = [...db.games]
        .sort((a, b) => (b.plays + b.likes.length * 2) - (a.plays + a.likes.length * 2))
        .slice(0, 4)
        .map(g => ({ id: g.id, title: g.title, authorName: g.authorName, genre: g.genre, likes: g.likes.length, plays: g.plays, groupId: g.groupId }));
    res.json(trending);
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

app.post('/api/games/:id/play', requireAuth, (req, res) => {
    const user = db.users.find(u => u.id === req.userId);
    const gameId = req.params.id;
    const game = db.games.find(g => g.id === gameId);
    
    if (game) {
        game.plays = (game.plays || 0) + 1;
        if (game.groupId) {
            const group = db.groups.find(gr => gr.id === game.groupId);
            if (group) {
                group.coins = (group.coins || 0) + 5;
                addGroupXp(group, 10);
            }
        }
    }

    user.coins += 5; // Always reward user
    user.recentlyPlayed = user.recentlyPlayed.filter(g => g.gameId !== gameId);
    user.recentlyPlayed.unshift({ gameId, timestamp: Date.now() });
    if (user.recentlyPlayed.length > 8) user.recentlyPlayed.pop();

    awardBadge(req.userId, 'Gamer');
    saveDB();
    res.json({ success: true, coins: user.coins });
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

    const { gameData, genre, lastLocalEditTime } = req.body;
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
        game.gameData = gameData;
        if (genre) game.genre = genre;
        game.lastEditTime = lastLocalEditTime;
        saveDB(); appliedUpdate = true;
    }

    res.json({ gameData: game.gameData, genre: game.genre, lastEditTime: game.lastEditTime, activeEditors: activeUsernames, acceptedLocalUpdate: appliedUpdate });
});

app.post('/api/games/:id/play-sync', requireAuth, (req, res) => {
    const gameId = req.params.id;
    const { x, y, z, rotY, sceneId, color } = req.body;
    const user = db.users.find(u => u.id === req.userId);

    if (!activePlayers[gameId]) activePlayers[gameId] = {};
    activePlayers[gameId][req.userId] = { 
        x, y, z, rotY, sceneId, username: user.username, 
        color: color || user.color || '#e74c3c', 
        equipped: user.equipped,
        timestamp: Date.now() 
    };

    const others = [];
    for (let uId in activePlayers[gameId]) {
        if (Date.now() - activePlayers[gameId][uId].timestamp < 3000) {
            if (uId !== req.userId && activePlayers[gameId][uId].sceneId === sceneId) {
                others.push(activePlayers[gameId][uId]);
            }
        } else delete activePlayers[gameId][uId];
    }
    res.json(others);
});

app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, () => {
    console.log(`Playsculpt server running on http://localhost:${PORT}`);
});