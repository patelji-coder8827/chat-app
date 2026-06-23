const mysql = require('mysql2');
require('dotenv').config();
const cors = require('cors');
const express = require('express');
const { Server } = require('socket.io');
const http = require('http');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs');
const path = require('path');
const nodemailer = require('nodemailer');
const jwt = require('jsonwebtoken');
const multer = require('multer');


const app = express();
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));
 

const port = process.env.PORT || 5000;
 
const server = http.createServer(app);

const io = new Server(server, {
    cors: {
        origin: process.env.FRONTEND_URL,
        methods: ["GET", "POST"],
        credentials: true
    }
});

const Chat = mysql.createConnection({
    host: process.env.DB_HOST,
    user: process.env.DB_USER, 
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    port: Number(process.env.DB_PORT),
    ssl: {
        ca: fs.readFileSync(process.env.DB_SSL_CA_PATH)
    }
});
console.log("HOST:", process.env.DB_HOST);
console.log("PORT:", process.env.DB_PORT);
console.log("SSL FILE:", process.env.DB_SSL_CA_PATH);

Chat.connect((error) => {
    if (error) {
        console.log("error connecting mysql:", error);
    } else {
        console.log("connected to mysql database");
    }
});

app.use(cors({
    origin: process.env.FRONTEND_URL, // This must be a specific URL, e.g., 'http://localhost:5173'
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization'],
    credentials: true, // This requires 'origin' to be a specific host, not '*'
}));


const uploadDir = path.join(__dirname, 'uploads');
if (!fs.existsSync(uploadDir)) {
    fs.mkdirSync(uploadDir);
}
app.use('/uploads', express.static(uploadDir));

const activeUsers = new Map();
const userBySocketId = new Map();
const onlineUsers = new Map();
const offlineMessages = new Map();

io.on('connection', (socket) => {
    console.log('A user connected:', socket.id);

    socket.on('registerUser', (userId) => {
        const userStrId = String(userId);
        activeUsers.set(userStrId, socket.id);
        userBySocketId.set(socket.id, userStrId);
        onlineUsers.set(userStrId, true);
        io.emit('userStatus', { userId: userStrId, isOnline: true });
        console.log(`User ${userStrId} registered with socket ID ${socket.id}`);

        if (offlineMessages.has(userStrId)) {
            const pendingMessages = offlineMessages.get(userStrId);
            pendingMessages.forEach(msg => {
                io.to(socket.id).emit('message', msg);
            });
            offlineMessages.delete(userStrId);
        }
    });

    socket.on('message', (msg) => {
        console.log('Received message:', msg);
            console.log(msg.image); 

        let finalMessage = { ...msg };

        const saveAndSend = (finalMsg) => {
            const isOnline = activeUsers.has(String(finalMsg.receiverId));
            const isDelivered = isOnline ? 1 : 0;


            const sqlQuery = `
                INSERT INTO messages 
                    (senderId, receiverId, text, image, timestamp, isDelivered) 
                VALUES 
                    (?, ?, ?, ?, NOW(), ?)
            `;

            const queryValues = [
                Number(finalMsg.senderId),
                Number(finalMsg.receiverId),
                finalMsg.text,
                finalMsg.image,
                isDelivered
            ];

            Chat.query(sqlQuery, queryValues, (error, result) => {
                if (error) {
                    console.error('Error saving message to database:', error);
                    io.to(socket.id).emit('userNotFound', { message: 'Failed to send message.' });
                    return;
                }

                finalMsg.id = result.insertId;
                finalMsg.createdAt = new Date().toISOString();
                

                if (isOnline) {
                    const receiverSocketId = activeUsers.get(String(finalMsg.receiverId));
                    io.to(receiverSocketId).emit('message', finalMsg);
                }

                io.to(socket.id).emit('message', finalMsg);
            });
        };

        if (finalMessage.image) {
            const base64Data = finalMessage.image.split(';base64,').pop();
            const filename = `${uuidv4()}.png`;
            const filepath = path.join(uploadDir, filename);
            

            fs.writeFile(filepath, base64Data, 'base64', (err) => {
                if (err) {
                    console.error("Error saving image:", err);
                    io.to(socket.id).emit('userNotFound', { message: 'Failed to send image.' });
                    return;
                }
                finalMessage.image = `${process.env.BACKEND_URL}/uploads/${filename}`;
                saveAndSend(finalMessage);
            });
        } else {
            saveAndSend(finalMessage);
        }
         console.log("Saved image URL:", finalMessage.image);
    });


    socket.on('deleteMessage', ({ messageId, deleteType }) => {
        const userId = userBySocketId.get(socket.id);
        if (!userId) return;

        if (deleteType === 'forMe') {
            const sql = 'INSERT INTO deleted_messages (message_id, user_id) VALUES (?, ?)';
            Chat.query(sql, [messageId, userId], (err, result) => {
                if (err) {
                    console.error('Error marking message as deleted for me:', err);
                    return;
                }

                socket.emit('messageDeleted', { messageId, deleteType: 'forMe' });
            });
        } else if (deleteType === 'forEveryone') {
            const getMessageSql = 'SELECT senderId, receiverId FROM messages WHERE id = ?';
            Chat.query(getMessageSql, [messageId], (err, messages) => {
                if (err || messages.length === 0) {
                    console.error('Message not found for deletion:', err);
                    return;
                }

                const message = messages[0];

                if (String(message.senderId) !== String(userId)) {
                    console.log(`User ${userId} attempted to delete a message they did not send.`);
                    return;
                }

                const updateSql = 'UPDATE messages SET text = NULL, image = NULL, isDeletedForAll = TRUE WHERE id = ?';
                Chat.query(updateSql, [messageId], (err, result) => {
                    if (err) {
                        console.error('Error deleting message for everyone:', err);
                        return;
                    }

                    const senderSocketId = activeUsers.get(String(message.senderId));
                    const receiverSocketId = activeUsers.get(String(message.receiverId));

                    if (senderSocketId) {
                        io.to(senderSocketId).emit('messageDeleted', { messageId, deleteType: 'forEveryone' });
                    }
                    if (receiverSocketId) {
                        io.to(receiverSocketId).emit('messageDeleted', { messageId, deleteType: 'forEveryone' });
                    }
                });
            });
        }
    });


    socket.on('addReaction', (data) => {
        const { messageId, userId, emoji } = data;

        const findMessageSql = 'SELECT senderId, receiverId FROM messages WHERE id = ?';
        Chat.query(findMessageSql, [messageId], (findErr, messages) => {
            if (findErr || messages.length === 0) {
                console.error('Message not found for reaction:', findErr);
                return;
            }
            const message = messages[0];


            const checkSql = 'SELECT id FROM reactions WHERE message_id = ? AND user_id = ? AND emoji = ?';
            Chat.query(checkSql, [messageId, userId, emoji], (checkErr, results) => {
                if (checkErr) {
                    console.error('Error checking reaction:', checkErr);
                    return;
                }

                const isReacted = results.length > 0;
                const eventName = isReacted ? 'reactionRemoved' : 'reactionAdded';
                let query, queryParams;

                if (isReacted) {
                    query = 'DELETE FROM reactions WHERE id = ?';
                    queryParams = [results[0].id];
                } else {
                    query = 'INSERT INTO reactions (message_id, user_id, emoji) VALUES (?, ?, ?)';
                    queryParams = [messageId, userId, emoji];
                }

                Chat.query(query, queryParams, (err, result) => {
                    if (err) {
                        console.error(`Error processing reaction (${eventName}):`, err);
                        return;
                    }


                    const senderSocketId = activeUsers.get(String(message.senderId));
                    const receiverSocketId = activeUsers.get(String(message.receiverId));


                    if (senderSocketId) io.to(senderSocketId).emit(eventName, data);
                    if (receiverSocketId) io.to(receiverSocketId).emit(eventName, data);
                });
            });
        });
    });


    socket.on('disconnect', () => {
        const userId = userBySocketId.get(socket.id);
        if (userId) {
            activeUsers.delete(userId);
            userBySocketId.delete(socket.id);
            onlineUsers.delete(userId);
            io.emit('userStatus', { userId: userId, isOnline: false });
            console.log(`User ${userId} disconnected.`);
        }
    });

    socket.on('requestOnlineUsers', () => {
        const onlineUserIds = Array.from(onlineUsers.keys());
        socket.emit('onlineUsersList', onlineUserIds);
    });
});



app.get('/messages/:senderId/:receiverId', (request, response) => {
    const { senderId, receiverId } = request.params;
    const sqlQuery = `
        SELECT 
            m.id, m.senderId, m.receiverId, m.text, m.image, m.timestamp, m.isDelivered, m.isDeletedForAll,
            (SELECT GROUP_CONCAT(CONCAT(r.emoji, ':', r.user_id)) 
             FROM reactions r WHERE r.message_id = m.id) as reactions_data
        FROM messages m
        LEFT JOIN deleted_messages dm ON m.id = dm.message_id AND dm.user_id = ?
        WHERE ((m.senderId = ? AND m.receiverId = ?) OR (m.senderId = ? AND m.receiverId = ?))
        AND dm.id IS NULL
        ORDER BY m.timestamp ASC
    `;
    const queryValues = [senderId, senderId, receiverId, receiverId, senderId];

    Chat.query(sqlQuery, queryValues, (error, results) => {
        if (error) return response.status(500).json({ message: 'Internal Server Error' });

        const formattedMessages = results.map(msg => {
            const reactions = {};
            if (msg.reactions_data) {
                msg.reactions_data.split(',').forEach(pair => {
                    const [emoji, userId] = pair.split(':');
                    if (!reactions[emoji]) reactions[emoji] = [];
                    reactions[emoji].push(userId);
                });
            }
            msg.reactions = reactions;
            msg.createdAt = new Date(msg.timestamp).toISOString();
            if (msg.isDeletedForAll) msg.isDeleted = true;
            return msg;
        });
        response.status(200).json({ messages: formattedMessages });
    });
});
/* Signup */
app.post('/signup', (request, response) => {
    const { FullName, email, password, confirm_password } = request.body;
    const sqlQuery = 'INSERT INTO signup (FullName, email, password, confirm_password) VALUES (?, ?, ?, ?)';

    Chat.query(sqlQuery, [FullName, email, password, confirm_password], (error, result) => {
        if (error) {
            console.log("error in query", error);
            response.status(500).json({ message: "An error occurred." });
        } else {
            console.log("data inserted");
            response.json({ message: "signup successful", userId: result.insertId });
        }
    });
});

/* SignIn */
app.post('/signin', (request, response) => {
    const { email, password } = request.body;
    const sqlQuery = 'SELECT * FROM signup WHERE email=? AND password=?';
    Chat.query(sqlQuery, [email, password], (error, result) => {
        if (error) {
            console.error("Error in query:", error);
            response.status(500).send({ message: "An error occurred." });
        } else {
            if (result.length > 0) {
                const user = result[0];
                response.json({
                    message: "signin successful",
                    user: {
                        id: user.id,
                        FullName: user.FullName,
                        email: user.email,
                    }
                });
            } else {
                response.status(401).json({ message: "invalid email or password" });
            }
        }
    });
});

const transporter = nodemailer.createTransport({
    host: 'smtp.gmail.com',
    port: 465,
    secure: true,
    auth: {
        user: process.env.EMAIL_USER,
        pass: process.env.EMAIL_PASS,
    },
    connectionTimeout: 10000,
    greetingTimeout: 10000,
    socketTimeout: 10000,
});


// forgetpassword
app.post('/Forget_password', (request, response) => {
    const email = request.body.email?.trim().toLowerCase();
    if (!email) return response.status(400).send({ message: "Email is required." });

    const sqlQuery = 'SELECT id, email FROM signup WHERE LOWER(email) = ?';
    Chat.query(sqlQuery, [email], (err, result) => {
        if (err) {
            console.error(err);
    
            return response.status(500).send({ message: "Database error." }); 
        }
        
        if (result.length === 0) {
            
            return response.status(404).send({ message: "User not found." });
        }

        const user = result[0];
        const jwtSecret = process.env.JWT_SECRET;

        
        
        const reset_token = jwt.sign({ id: user.id, email: user.email }, jwtSecret, { expiresIn: '10m' });
        const decoded = jwt.verify(reset_token, jwtSecret);
        console.log("Decoded token:", decoded);

        const resetLink = `${process.env.FRONTEND_URL}/reset-password?token=${reset_token}`;

        const mailOptions = {
            from: 'realankitpatel@gmail.com',
            to: user.email,
            subject: 'Password Reset Request',
            html: `<p>You requested a password reset. Click the following link to reset your password:</p>
                   <a href="${resetLink}">Reset Password</a>`
        };

        transporter.sendMail(mailOptions, (mailError, info) => {
            if (mailError) {
                console.error("Error sending email:", mailError);
                return response.status(500).send({ message: "Failed to send reset email. Please try again later." });
            }
            console.log('Message sent: %s', info.messageId);
            response.send({ message: "Message sent in gmail", user: user });
        });
    });
});
app.post('/reset_password', (request, response) => {
    const { token, newPassword } = request.body;

    try {
        const decoded = jwt.verify(token, process.env.JWT_SECRET);
        const userId = decoded.id;


        const sqlQuery = 'UPDATE signup SET password = ? WHERE id = ?';
        Chat.query(sqlQuery, [newPassword, userId], (error, result) => {
            if (error) {
                console.error("Error updating password:", error);
                return response.status(500).send({ message: "An error occurred while updating the password." });
            }
            if (result.affectedRows === 0) {
                return response.status(404).send({ message: "User not found or password not updated." });
            }
            response.status(200).send({ message: "Password updated successfully!" });
        });

    } catch (error) {
        console.error("Token verification failed:", error);
        return response.status(401).send({ message: "Invalid or expired token. Please try again." });
    }
}); 

/* User details & other Express routes */
app.get('/user/:userId', (request, response) => {
    const userId = request.params.userId;
    const sqlQuery = 'SELECT FullName, bio, profilePic FROM signup WHERE id = ?';
    Chat.query(sqlQuery, [userId], (error, result) => {
        if (error) {
            console.error("Error fetching user data:", error);
            response.status(500).send({ message: "Error fetching user data." });
        } else if (result.length > 0) {
            response.send({
                message: "User data fetched successfully",
                user: result[0]
            });
        } else {
            response.status(404).send({ message: "User not found" });
        }
    });
});

app.get('/users', (req, res) => {
    const sqlQuery = 'SELECT id, FullName, profilePic FROM signup';
    Chat.query(sqlQuery, (error, results) => {
        if (error) {
            console.error('Error fetching users:', error);
            return res.status(500).json({ message: 'Internal Server Error' });
        }
        res.status(200).json({ users: results });
    });
});

app.post('/update-bio', (request, response) => {
    const { userId, bio } = request.body;
    const sqlQuery = 'UPDATE signup SET bio = ? WHERE id = ?';
    Chat.query(sqlQuery, [bio, userId], (error, result) => {
        if (error) {
            console.error("Error updating bio:", error);
            response.status(500).send({ message: "An error occurred while updating the bio." });
        } else if (result.affectedRows === 0) {
            response.status(404).send({ message: "User not found." });
        } else {
            response.send({ message: "Bio updated successfully!" });
        }
    });
});

app.post('/update-profile', (request, response) => {
    const { userId, fullName, bio, profilePic } = request.body;
    const sqlQuery = 'UPDATE signup SET FullName = ?, bio = ?, profilePic = ? WHERE id = ?';
    Chat.query(sqlQuery, [fullName, bio, profilePic, userId], (error, result) => {
        if (error) {
            console.error("Error updating profile:", error);
            response.status(500).send({ message: "An error occurred while updating the profile." });
        } else if (result.affectedRows === 0) {
            response.status(404).send({ message: "User not found." });
        } else {
            response.send({ message: "Profile updated successfully!" });
        }
    });
});




const statusUploadDir = path.join(__dirname, 'status_uploads');
if (!fs.existsSync(statusUploadDir)) {
    fs.mkdirSync(statusUploadDir);
}


app.use('/status_uploads', express.static(statusUploadDir));

const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        cb(null, statusUploadDir);
    },
    filename: (req, file, cb) => {
        const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9);
        const fileExtension = path.extname(file.originalname);
        cb(null, file.fieldname + '-' + uniqueSuffix + fileExtension);
    }
});

const upload = multer({ storage: storage });

app.post('/api/statuses', upload.single('statusMedia'), (req, res) => {
    const { userId, text } = req.body;
    const mediaFile = req.file;

    
    if (!userId || (!text && !mediaFile)) {
        return res.status(400).json({ message: "User ID, text, or media is required." });
    }

    // Construct the correct URL for the media file
    const mediaPath = mediaFile ? `/status_uploads/${mediaFile.filename}` : null;

    const sqlQuery = 'INSERT INTO statuses (user_id, text, media, timestamp) VALUES (?, ?, ?, NOW())';
    const queryValues = [userId, text || null, mediaPath];

    Chat.query(sqlQuery, queryValues, (error, result) => {
        if (error) {
            console.error("Error saving status:", error);

            if (mediaFile) {
                fs.unlink(mediaFile.path, err => {
                    if (err) console.error("Error deleting file after database failure:", err);
                });
            }
            return res.status(500).json({ message: "Failed to save status." });
        }

        const newStatus = {
            id: result.insertId,
            user_id: userId,
            text: text || null,
            media: mediaPath,
            timestamp: new Date().toISOString()
        };
        res.status(201).json({ message: "Status added successfully!", status: newStatus });
    });
});

app.get('/api/statuses', (req, res) => {
    const sqlQuery = `
        SELECT s.id, s.user_id, s.text, s.media, s.timestamp, u.FullName, u.profilePic
        FROM statuses s
        JOIN signup u ON s.user_id = u.id
        ORDER BY s.timestamp DESC
    `;
    Chat.query(sqlQuery, (error, results) => {
        if (error) {
            console.error("Error fetching statuses:", error);
            return res.status(500).json({ message: "Internal Server Error" });
        }
        res.status(200).json({ statuses: results });
    });
});

app.delete('/api/statuses/:statusId', (req, res) => {
    const { statusId } = req.params;
    const { userId } = req.body;

    const sqlQuery = 'DELETE FROM statuses WHERE id = ? AND user_id = ?';
    Chat.query(sqlQuery, [statusId, userId], (error, result) => {
        if (error) {
            console.error("Error deleting status:", error);
            return res.status(500).json({ message: "Internal Server Error" });
        }
        if (result.affectedRows === 0) {
            return res.status(404).json({ message: "Status not found or user not authorized." });
        }
        res.status(200).json({ message: "Status deleted successfully." });
    });
});
server.listen(port, () => {
    console.log(`Server running on port: ${port}`);
});