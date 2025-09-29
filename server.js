const express = require('express');
const WebSocket = require('ws');
const http = require('http');
const cors = require('cors');

const app = express();
const port = process.env.PORT || 3000;

// Enable CORS for all routes
app.use(cors());
app.use(express.json());

// Create HTTP server
const server = http.createServer(app);

// Create WebSocket server
const wss = new WebSocket.Server({ server });

// Store connected clients with their peer IDs
const clients = new Map();

// Basic route for health check
app.get('/', (req, res) => {
    res.json({ 
        message: 'WebRTC Signaling Server is running!', 
        connectedClients: clients.size,
        timestamp: new Date().toISOString()
    });
});

// Get list of connected clients
app.get('/clients', (req, res) => {
    const clientList = Array.from(clients.keys());
    res.json({ clients: clientList, count: clientList.length });
});

// WebSocket connection handler
wss.on('connection', function connection(socket, request) {
    console.log('New WebSocket connection established');
    
    let clientPeerId = null;

    // Handle incoming messages
    socket.on('message', function message(data) {
        try {
            const messageStr = data.toString();
            console.log('Received message type:', messageStr.substring(0, 50) + '...');
            
            // Handle test messages (like "TEST!WEBSOCKET!TEST")
            if (messageStr.includes('TEST!WEBSOCKET!TEST')) {
                console.log('Test message received:', messageStr);
                // Echo back the test message
                socket.send(`Echo: ${messageStr}`);
                return;
            }
            
            // Handle voice messages - these are large and should be forwarded directly
            if (messageStr.startsWith('VOICE_MESSAGE:')) {
                console.log('Voice message detected, forwarding to other clients...');
                handleVoiceMessage(messageStr, socket);
                return;
            }
            
            // Try to parse as JSON for WebRTC signaling
            let messageObj;
            try {
                messageObj = JSON.parse(messageStr);
            } catch (e) {
                // If it's not JSON, treat as plain text message and broadcast
                console.log('Plain text message, broadcasting...');
                broadcastToAllClients(messageStr, socket);
                return;
            }
            
            // Handle different types of signaling messages
            switch (messageObj.type) {
                case 'register':
                    handleRegister(socket, messageObj);
                    break;
                case 'offer':
                    handleOffer(messageObj);
                    break;
                case 'answer':
                    handleAnswer(messageObj);
                    break;
                case 'ice-candidate':
                    handleIceCandidate(messageObj);
                    break;
                case 'data-channel':
                    handleDataChannel(messageObj);
                    break;
                default:
                    console.log('Unknown message type:', messageObj.type);
                    // Broadcast unknown messages to all other clients
                    broadcastToOthers(JSON.stringify(messageObj), socket);
                    break;
            }
        } catch (error) {
            console.error('Error processing message:', error);
        }
    });

    // Handle client disconnect
    socket.on('close', function close() {
        console.log(`Client ${clientPeerId} disconnected`);
        if (clientPeerId) {
            clients.delete(clientPeerId);
            // Notify other clients about disconnection
            broadcastToAllClients(JSON.stringify({
                type: 'client-disconnected',
                peerId: clientPeerId,
                timestamp: new Date().toISOString()
            }), socket);
        }
    });

    // Handle errors
    socket.on('error', function error(err) {
        console.error('WebSocket error:', err);
    });

    // Send welcome message
    socket.send(JSON.stringify({
        type: 'welcome',
        message: 'Connected to WebRTC signaling server',
        timestamp: new Date().toISOString()
    }));

    // Function to handle voice messages
    function handleVoiceMessage(message, senderSocket) {
        try {
            // Extract sender ID from the voice message format
            // Format: VOICE_MESSAGE:senderId:base64AudioData
            const parts = message.split(':');
            if (parts.length >= 3) {
                const senderId = parts[1];
                console.log(`Voice message from ${senderId}, size: ${message.length} bytes`);
                
                // Forward to all other clients
                let recipientCount = 0;
                clients.forEach((clientSocket, peerId) => {
                    if (clientSocket !== senderSocket && clientSocket.readyState === WebSocket.OPEN) {
                        clientSocket.send(message);
                        recipientCount++;
                    }
                });
                
                console.log(`Voice message forwarded to ${recipientCount} recipients`);
            } else {
                console.error('Invalid voice message format');
            }
        } catch (error) {
            console.error('Error handling voice message:', error);
        }
    }

    // Function to handle client registration
    function handleRegister(socket, message) {
        clientPeerId = message.peerId || `client_${Date.now()}`;
        clients.set(clientPeerId, socket);
        
        console.log(`Client registered with ID: ${clientPeerId}`);
        
        // Send registration confirmation
        socket.send(JSON.stringify({
            type: 'registered',
            peerId: clientPeerId,
            message: 'Successfully registered'
        }));
        
        // Notify other clients about new connection
        broadcastToOthers(JSON.stringify({
            type: 'client-connected',
            peerId: clientPeerId,
            timestamp: new Date().toISOString()
        }), socket);
        
        // Send list of other connected clients
        const otherClients = Array.from(clients.keys()).filter(id => id !== clientPeerId);
        socket.send(JSON.stringify({
            type: 'client-list',
            clients: otherClients
        }));
    }

    // Function to handle WebRTC offers
    function handleOffer(message) {
        console.log(`Forwarding offer from ${message.sender} to ${message.target}`);
        const targetSocket = clients.get(message.target);
        if (targetSocket && targetSocket.readyState === WebSocket.OPEN) {
            targetSocket.send(JSON.stringify(message));
        } else {
            console.log(`Target client ${message.target} not found or not connected`);
        }
    }

    // Function to handle WebRTC answers
    function handleAnswer(message) {
        console.log(`Forwarding answer from ${message.sender} to ${message.target}`);
        const targetSocket = clients.get(message.target);
        if (targetSocket && targetSocket.readyState === WebSocket.OPEN) {
            targetSocket.send(JSON.stringify(message));
        } else {
            console.log(`Target client ${message.target} not found or not connected`);
        }
    }

    // Function to handle ICE candidates
    function handleIceCandidate(message) {
        console.log(`Forwarding ICE candidate from ${message.sender} to ${message.target}`);
        const targetSocket = clients.get(message.target);
        if (targetSocket && targetSocket.readyState === WebSocket.OPEN) {
            targetSocket.send(JSON.stringify(message));
        } else {
            console.log(`Target client ${message.target} not found or not connected`);
        }
    }

    // Function to handle data channel messages
    function handleDataChannel(message) {
        console.log(`Data channel message from ${message.sender}`);
        if (message.target) {
            // Send to specific target
            const targetSocket = clients.get(message.target);
            if (targetSocket && targetSocket.readyState === WebSocket.OPEN) {
                targetSocket.send(JSON.stringify(message));
            }
        } else {
            // Broadcast to all other clients
            broadcastToOthers(JSON.stringify(message), socket);
        }
    }
});

// Function to broadcast message to all clients except sender
function broadcastToOthers(message, senderSocket) {
    wss.clients.forEach(function each(client) {
        if (client !== senderSocket && client.readyState === WebSocket.OPEN) {
            client.send(message);
        }
    });
}

// Function to broadcast message to all clients
function broadcastToAllClients(message, senderSocket = null) {
    wss.clients.forEach(function each(client) {
        if (client.readyState === WebSocket.OPEN) {
            if (senderSocket === null || client !== senderSocket) {
                client.send(message);
            }
        }
    });
}

// Start the server
server.listen(port, function listening() {
    console.log(`WebRTC Signaling Server is running on port ${port}`);
    console.log(`WebSocket endpoint: ws://localhost:${port}`);
    console.log(`HTTP endpoint: http://localhost:${port}`);
});

// Graceful shutdown
process.on('SIGTERM', () => {
    console.log('Received SIGTERM, shutting down gracefully');
    server.close(() => {
        console.log('Server closed');
        process.exit(0);
    });
});

process.on('SIGINT', () => {
    console.log('Received SIGINT, shutting down gracefully');
    server.close(() => {
        console.log('Server closed');
        process.exit(0);
    });
});











































// const express = require('express');
// const WebSocket = require('ws');
// const http = require('http');
// const cors = require('cors');

// const app = express();
// const port = process.env.PORT || 3000;

// // Enable CORS for all routes
// app.use(cors());
// app.use(express.json());

// // Create HTTP server
// const server = http.createServer(app);

// // Create WebSocket server
// const wss = new WebSocket.Server({ server });

// // Store connected clients with their peer IDs
// const clients = new Map();

// // Basic route for health check
// app.get('/', (req, res) => {
//     res.json({ 
//         message: 'WebRTC Signaling Server is running!', 
//         connectedClients: clients.size,
//         timestamp: new Date().toISOString()
//     });
// });

// // Get list of connected clients
// app.get('/clients', (req, res) => {
//     const clientList = Array.from(clients.keys());
//     res.json({ clients: clientList, count: clientList.length });
// });

// // WebSocket connection handler
// wss.on('connection', function connection(socket, request) {
//     console.log('New WebSocket connection established');
    
//     let clientPeerId = null;

//     // Handle incoming messages
//     socket.on('message', function message(data) {
//         try {
//             const messageStr = data.toString();
//             console.log('Received message:', messageStr);
            
//             // Handle test messages (like "TEST!WEBSOCKET!TEST")
//             if (messageStr.includes('TEST!WEBSOCKET!TEST')) {
//                 console.log('Test message received:', messageStr);
//                 // Echo back the test message
//                 socket.send(`Echo: ${messageStr}`);
//                 return;
//             }
            
//             // Try to parse as JSON for WebRTC signaling
//             let messageObj;
//             try {
//                 messageObj = JSON.parse(messageStr);
//             } catch (e) {
//                 // If it's not JSON, treat as plain text message
//                 console.log('Plain text message:', messageStr);
//                 broadcastToAllClients(messageStr, socket);
//                 return;
//             }
            
//             // Handle different types of signaling messages
//             switch (messageObj.type) {
//                 case 'register':
//                     handleRegister(socket, messageObj);
//                     break;
//                 case 'offer':
//                     handleOffer(messageObj);
//                     break;
//                 case 'answer':
//                     handleAnswer(messageObj);
//                     break;
//                 case 'ice-candidate':
//                     handleIceCandidate(messageObj);
//                     break;
//                 case 'data-channel':
//                     handleDataChannel(messageObj);
//                     break;
//                 default:
//                     console.log('Unknown message type:', messageObj.type);
//                     // Broadcast unknown messages to all other clients
//                     broadcastToOthers(JSON.stringify(messageObj), socket);
//                     break;
//             }
//         } catch (error) {
//             console.error('Error processing message:', error);
//         }
//     });

//     // Handle client disconnect
//     socket.on('close', function close() {
//         console.log(`Client ${clientPeerId} disconnected`);
//         if (clientPeerId) {
//             clients.delete(clientPeerId);
//             // Notify other clients about disconnection
//             broadcastToAllClients(JSON.stringify({
//                 type: 'client-disconnected',
//                 peerId: clientPeerId,
//                 timestamp: new Date().toISOString()
//             }), socket);
//         }
//     });

//     // Handle errors
//     socket.on('error', function error(err) {
//         console.error('WebSocket error:', err);
//     });

//     // Send welcome message
//     socket.send(JSON.stringify({
//         type: 'welcome',
//         message: 'Connected to WebRTC signaling server',
//         timestamp: new Date().toISOString()
//     }));

//     // Function to handle client registration
//     function handleRegister(socket, message) {
//         clientPeerId = message.peerId || `client_${Date.now()}`;
//         clients.set(clientPeerId, socket);
        
//         console.log(`Client registered with ID: ${clientPeerId}`);
        
//         // Send registration confirmation
//         socket.send(JSON.stringify({
//             type: 'registered',
//             peerId: clientPeerId,
//             message: 'Successfully registered'
//         }));
        
//         // Notify other clients about new connection
//         broadcastToOthers(JSON.stringify({
//             type: 'client-connected',
//             peerId: clientPeerId,
//             timestamp: new Date().toISOString()
//         }), socket);
        
//         // Send list of other connected clients
//         const otherClients = Array.from(clients.keys()).filter(id => id !== clientPeerId);
//         socket.send(JSON.stringify({
//             type: 'client-list',
//             clients: otherClients
//         }));
//     }

//     // Function to handle WebRTC offers
//     function handleOffer(message) {
//         console.log(`Forwarding offer from ${message.sender} to ${message.target}`);
//         const targetSocket = clients.get(message.target);
//         if (targetSocket && targetSocket.readyState === WebSocket.OPEN) {
//             targetSocket.send(JSON.stringify(message));
//         } else {
//             console.log(`Target client ${message.target} not found or not connected`);
//         }
//     }

//     // Function to handle WebRTC answers
//     function handleAnswer(message) {
//         console.log(`Forwarding answer from ${message.sender} to ${message.target}`);
//         const targetSocket = clients.get(message.target);
//         if (targetSocket && targetSocket.readyState === WebSocket.OPEN) {
//             targetSocket.send(JSON.stringify(message));
//         } else {
//             console.log(`Target client ${message.target} not found or not connected`);
//         }
//     }

//     // Function to handle ICE candidates
//     function handleIceCandidate(message) {
//         console.log(`Forwarding ICE candidate from ${message.sender} to ${message.target}`);
//         const targetSocket = clients.get(message.target);
//         if (targetSocket && targetSocket.readyState === WebSocket.OPEN) {
//             targetSocket.send(JSON.stringify(message));
//         } else {
//             console.log(`Target client ${message.target} not found or not connected`);
//         }
//     }

//     // Function to handle data channel messages
//     function handleDataChannel(message) {
//         console.log(`Data channel message from ${message.sender}: ${message.data}`);
//         if (message.target) {
//             // Send to specific target
//             const targetSocket = clients.get(message.target);
//             if (targetSocket && targetSocket.readyState === WebSocket.OPEN) {
//                 targetSocket.send(JSON.stringify(message));
//             }
//         } else {
//             // Broadcast to all other clients
//             broadcastToOthers(JSON.stringify(message), socket);
//         }
//     }
// });

// // Function to broadcast message to all clients except sender
// function broadcastToOthers(message, senderSocket) {
//     wss.clients.forEach(function each(client) {
//         if (client !== senderSocket && client.readyState === WebSocket.OPEN) {
//             client.send(message);
//         }
//     });
// }

// // Function to broadcast message to all clients
// function broadcastToAllClients(message, senderSocket = null) {
//     wss.clients.forEach(function each(client) {
//         if (client.readyState === WebSocket.OPEN) {
//             if (senderSocket === null || client !== senderSocket) {
//                 client.send(message);
//             }
//         }
//     });
// }

// // Start the server
// server.listen(port, function listening() {
//     console.log(`WebRTC Signaling Server is running on port ${port}`);
//     console.log(`WebSocket endpoint: ws://localhost:${port}`);
//     console.log(`HTTP endpoint: http://localhost:${port}`);
// });

// // Graceful shutdown
// process.on('SIGTERM', () => {
//     console.log('Received SIGTERM, shutting down gracefully');
//     server.close(() => {
//         console.log('Server closed');
//         process.exit(0);
//     });
// });

// process.on('SIGINT', () => {
//     console.log('Received SIGINT, shutting down gracefully');
//     server.close(() => {
//         console.log('Server closed');
//         process.exit(0);
//     });
// });