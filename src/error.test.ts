import { describe, it, expect } from "vitest";
import { TxOBError, ErrorUnprocessableEventHandler } from "./error.js";

describe("TxOBError", () => {
  it("sets message and optional backoffUntil", () => {
    const until = new Date("2030-01-01");
    const err = new TxOBError("rate limited", { backoffUntil: until });
    expect(err.message).toBe("rate limited");
    expect(err.backoffUntil).toBe(until);
  });

  it("preserves cause when provided", () => {
    const cause = new Error("upstream");
    const err = new TxOBError("wrapped", { cause });
    expect(err.cause).toBe(cause);
  });
});

describe("ErrorUnprocessableEventHandler", () => {
  it("wraps the original error and exposes it", () => {
    const inner = new Error("bad payload");
    const err = new ErrorUnprocessableEventHandler(inner);
    expect(err.message).toBe("unprocessable event handler: bad payload");
    expect(err.error).toBe(inner);
  });
});
