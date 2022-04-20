const { Kafka } = require("kafkajs");

const server = require("http").createServer();
const io = require("socket.io")(server, {
  cors: {
    origin: "*",
  },
});

const PORT = 4000;
const NEW_CHAT_MESSAGE_EVENT = "newChatMessage";
const clientId = "kafka-broker-app";
const brokers = ["localhost:9092"];

const kafka = new Kafka({ clientId, brokers });
const consumer = kafka.consumer({ groupId: clientId });

const consume = async () => {
  await consumer.connect();
  await consumer.subscribe({
    topic: "roomtopic44a1dece-38b9-4644-bc62-43e2a2fb8f13",
  });
  await consumer.run({
    eachMessage: ({ topic, partition, message }) => {
      console.log(topic + "Received Message from topic");
      console.log(`received message: ${message.value}`);
      io.in(topic).emit(NEW_CHAT_MESSAGE_EVENT, message.value);
    },
  });
};

consume().catch((err) => {
  console.error("error while creating consumer: ", err);
});

io.on("connection", (socket) => {
  console.log(`Client ${socket.id} connected`);

  // Join a conversation
  const { roomId } = socket.handshake.query;
  socket.join(roomId);

  // const subscribe = async (topicName) => {
  //   console.log("Subscribing for topic: " + topicName);
  //   await consumer.subscribe({ topic: topicName });
  // };

  // subscribe(roomId);

  // Listen for new messages
  socket.on(NEW_CHAT_MESSAGE_EVENT, (data) => {
    io.in(roomId).emit(NEW_CHAT_MESSAGE_EVENT, data);
  });

  // Leave the room if the user closes the socket
  socket.on("disconnect", () => {
    console.log(`Client ${socket.id} diconnected`);
    socket.leave(roomId);
  });
});

server.listen(PORT, () => {
  console.log(`Listening on port ${PORT}`);
});
