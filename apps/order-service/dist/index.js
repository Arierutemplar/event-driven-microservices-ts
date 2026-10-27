import "dotenv/config";
import express from "express";
import { createOrder } from "./controllers/index.js";
const app = express();
app.use(express.json());
app.post("/orders", createOrder);
const port = Number(process.env.PORT ?? "3000");
if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error("PORT must be an integer between 1 and 65535");
}
app.listen(port, () => {
    console.info(`OrderService listening on port ${port}`);
});
