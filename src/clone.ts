export function deepClone<T>(obj: T): T {
  return structuredClone(obj);
}
