import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import pino from "pino";
import { fileURLToPath } from "node:url";
const protoPath = fileURLToPath(new URL("../../proto/user.proto", import.meta.url));
const packageDefinition = protoLoader.loadSync(protoPath, {
    keepCase: false,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true
});
const proto = grpc.loadPackageDefinition(packageDefinition);
const logger = pino({ name: "user-service" });
const mockUser = {
    id: "1",
    name: "Ada Lovelace",
    email: "ada@example.com"
};
const server = new grpc.Server();
server.addService(proto.user.UserService.service, {
    getUserById: (call, callback) => {
        const correlationId = call.metadata.get("x-correlation-id")[0];
        logger.info({
            correlationId: typeof correlationId === "string" ? correlationId : undefined,
            userId: call.request.id
        }, "GetUserById request received");
        if (call.request.id !== mockUser.id) {
            callback({ code: grpc.status.NOT_FOUND, details: "User not found" }, null);
            return;
        }
        callback(null, mockUser);
    }
});
server.bindAsync("0.0.0.0:50051", grpc.ServerCredentials.createInsecure(), (error) => {
    if (error) {
        console.error("Failed to bind UserService:", error);
        process.exitCode = 1;
        return;
    }
    server.start();
    console.info("UserService listening on port 50051");
});
