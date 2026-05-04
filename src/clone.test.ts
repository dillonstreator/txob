import { describe, it, expect } from "vitest";
import { deepClone } from "./clone.js";

describe("deepClone", () => {
  it("returns an equal but distinct nested structure", () => {
    const input = { a: 1, nested: { b: [2, 3] } };
    const copy = deepClone(input);
    expect(copy).toEqual(input);
    expect(copy).not.toBe(input);
    expect(copy.nested).not.toBe(input.nested);
    expect(copy.nested.b).not.toBe(input.nested.b);
  });
});
