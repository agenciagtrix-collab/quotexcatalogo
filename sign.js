import { createHmac } from "node:crypto";

export function signBody(body, secret) {
  return createHmac("sha256", secret).update(body).digest("hex");
}
