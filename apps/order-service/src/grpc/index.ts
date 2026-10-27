import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import CircuitBreaker from "opossum";
import { fileURLToPath } from "node:url";

interface User {
  id: string;
  name: string;
  email: string;
}

interface UserServiceClient extends grpc.Client {
  getUserById(
    request: { id: string },
    metadata: grpc.Metadata,
    options: grpc.CallOptions,
    callback: (error: grpc.ServiceError | null, response?: User) => void
  ): grpc.ClientUnaryCall;
}

interface UserServiceConstructor extends grpc.ServiceClientConstructor {
  service: grpc.ServiceDefinition;
}

class UserServiceCircuitOpenError extends Error {
  readonly code = grpc.status.UNAVAILABLE;

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
const proto = grpc.loadPackageDefinition(packageDefinition) as unknown as {
  user: { UserService: UserServiceConstructor };
};
const UserServiceClientConstructor = proto.user.UserService;
const userClient = new UserServiceClientConstructor(
  process.env.USER_SERVICE_URL ?? "localhost:50051",
  grpc.credentials.createInsecure()
) as unknown as UserServiceClient;

function getUserAction(userId: string, metadata: grpc.Metadata): Promise<User> {
  return new Promise<User>((resolve, reject) => {
    userClient.getUserById(
      { id: userId },
      metadata,
      { deadline: Date.now() + 3000 },
      (error, user) => {
        if (error) {
          reject(error);
          return;
        }
        if (!user) {
          reject(new Error("UserService returned an empty response"));
          return;
        }
        resolve(user);
      }
    );
  });
}

const userServiceCircuit = new CircuitBreaker(getUserAction, {
  timeout: 3000,
  errorThresholdPercentage: 50
});

userServiceCircuit.fallback(
  (userId: string, metadata: grpc.Metadata, error: Error): Promise<never> => {
    if ("code" in error && error.code === "EOPENBREAKER") {
      return Promise.reject(new UserServiceCircuitOpenError());
    }

    return Promise.reject(error);
  }
);

export function getUser(userId: string, metadata: grpc.Metadata): Promise<User> {
  return userServiceCircuit.fire(userId, metadata);
}
