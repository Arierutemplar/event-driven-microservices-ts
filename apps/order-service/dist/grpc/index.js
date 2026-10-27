import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import CircuitBreaker from "opossum";
import { fileURLToPath } from "node:url";
class UserServiceCircuitOpenError extends Error {
    code = grpc.status.UNAVAILABLE;
    constructor() {
        super("User service is temporarily unavailable (circuit breaker is open)");
        this.name = "UserServiceCircuitOpenError";
    }
}
const protoPath = fileURLToPath(new URL("../../../proto/user.proto", import.meta.url));
const packageDefinition = protoLoader.loadSync(protoPath, {
    keepCase: false,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true
});
const proto = grpc.loadPackageDefinition(packageDefinition);
const UserServiceClientConstructor = proto.user.UserService;
const userClient = new UserServiceClientConstructor(process.env.USER_SERVICE_URL ?? "localhost:50051", grpc.credentials.createInsecure());
function getUserAction(userId, metadata) {
    return new Promise((resolve, reject) => {
        userClient.getUserById({ id: userId }, metadata, { deadline: Date.now() + 3000 }, (error, user) => {
            if (error) {
                reject(error);
                return;
            }
            if (!user) {
                reject(new Error("UserService returned an empty response"));
                return;
            }
            resolve(user);
        });
    });
}
const userServiceCircuit = new CircuitBreaker(getUserAction, {
    timeout: 3000,
    errorThresholdPercentage: 50
});
userServiceCircuit.fallback((userId, metadata, error) => {
    if ("code" in error && error.code === "EOPENBREAKER") {
        return Promise.reject(new UserServiceCircuitOpenError());
    }
    return Promise.reject(error);
});
export function getUser(userId, metadata) {
    return userServiceCircuit.fire(userId, metadata);
}
