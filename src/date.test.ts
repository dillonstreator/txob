import { describe, it, expect } from "vitest";
import { getDate } from "./date.js";

describe("getDate", () => {
  it("should return a Date object", () => {
    const result = getDate();
    expect(result).toBeInstanceOf(Date);
  });

  it("should return current time", () => {
    const before = Date.now();
    const result = getDate();
    const after = Date.now();

    expect(result.getTime()).toBeGreaterThanOrEqual(before);
    expect(result.getTime()).toBeLessThanOrEqual(after);
  });
});
