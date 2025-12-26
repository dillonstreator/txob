import { describe, it, expect, vi } from "vitest";
import { retryable } from "./retry.js";

describe("retryable", () => {
  it("should retry and succeed on second attempt", async () => {
    let attempts = 0;
    const action = vi.fn(async () => {
      attempts++;
      if (attempts < 2) {
        throw new Error("temporary error");
      }
      return "success";
    });

    const result = await retryable(action, {
      retries: 3,
      minTimeout: 1,
      maxTimeout: 10,
      factor: 1,
    });

    expect(result).toBe("success");
    expect(action).toHaveBeenCalledTimes(2);
  });

  it("should exhaust retries and reject", async () => {
    const error = new Error("persistent error");
    const action = vi.fn(async () => {
      throw error;
    });

    await expect(
      retryable(action, {
        retries: 2,
        minTimeout: 1,
        maxTimeout: 10,
        factor: 1,
      }),
    ).rejects.toThrow("persistent error");

    expect(action).toHaveBeenCalledTimes(3); // initial + 2 retries
  });

  it("should succeed on first attempt", async () => {
    const action = vi.fn(async () => {
      return "success";
    });

    const result = await retryable(action, {
      retries: 3,
      minTimeout: 1,
      maxTimeout: 10,
    });

    expect(result).toBe("success");
    expect(action).toHaveBeenCalledTimes(1);
  });
});
