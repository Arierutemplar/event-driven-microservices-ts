import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import pino from "pino";
import { fileURLToPath } from "node:url";

interface GetUserByIdRequest {
  id: string;
}

interface User {
  id: string;
  name: string;
  email: string;
}

interface UserServiceConstructor extends grpc.ServiceClientConstructor {
  service: grpc.ServiceDefinition;
}

const protoPath = fileURLToPath(new URL("../../proto/user.proto", import.meta.url));
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
const logger = pino({ name: "user-service" });

const mockUser: User = {
  id: "1",
  name: "Ada Lovelace",
  email: "ada@example.com"
};

const server = new grpc.Server();
server.addService(proto.user.UserService.service, {
  getUserById: (
    call: grpc.ServerUnaryCall<GetUserByIdRequest, User>,
    callback: grpc.sendUnaryData<User>
  ): void => {
    const correlationId = call.metadata.get("x-correlation-id")[0];
    logger.info(
      {
        correlationId: typeof correlationId === "string" ? correlationId : undefined,
        userId: call.request.id
      },
      "GetUserById request received"
    );

    if (call.request.id !== mockUser.id) {
      callback(
        { code: grpc.status.NOT_FOUND, details: "User not found" },
        null
      );
      return;
    }

    callback(null, mockUser);
  }
});

server.bindAsync(
  "0.0.0.0:50051",
  grpc.ServerCredentials.createInsecure(),
  (error): void => {
    if (error) {
      console.error("Failed to bind UserService:", error);
      process.exitCode = 1;
      return;
    }

    server.start();
    console.info("UserService listening on port 50051");
  }
);