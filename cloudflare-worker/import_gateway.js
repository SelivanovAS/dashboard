// Только явно настроенный шлюз территории может отдавать тело обратным запросом.
// Ключ роли остаётся в исходном запросе; скачивание использует отдельный токен.
const MAX_BYTES = 10 * 1024 * 1024;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const HEX64 = /^[0-9a-f]{64}$/;

class GatewayError extends Error {
  constructor(status, message, sha256 = "") {
    super(message);
    this.status = status;
    this.sha256 = sha256;
  }
}

export async function readGatewayImportBody(envelope, env) {
  if (!env.IMPORT_GATEWAY_ORIGIN) throw new GatewayError(400, "Обратная загрузка на этой территории выключена");
  const ref = envelope && envelope.__gateway_upload;
  if (!ref || Array.isArray(ref) || Object.keys(envelope).length !== 1
      || typeof ref.id !== "string" || typeof ref.token !== "string" || typeof ref.sha256 !== "string"
      || !UUID.test(ref.id) || !HEX64.test(ref.token) || !HEX64.test(ref.sha256)
      || !Number.isSafeInteger(ref.bytes) || ref.bytes < 1 || ref.bytes > MAX_BYTES) {
    throw new GatewayError(400, "Неверные параметры обратной загрузки");
  }
  let origin;
  try {
    origin = new URL(env.IMPORT_GATEWAY_ORIGIN);
    if (origin.protocol !== "https:" || origin.username || origin.password
        || origin.pathname !== "/" || origin.search || origin.hash || origin.port) throw new Error();
  } catch (_) {
    throw new GatewayError(502, "Неверная настройка адреса шлюза загрузки");
  }
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 20000);
  let reader;
  try {
    const response = await fetch(origin.origin + "/_gateway-upload/" + ref.id, {
      method: "GET", headers: { Authorization: "Bearer " + ref.token },
      redirect: "manual", signal: controller.signal,
    });
    if (response.status !== 200 || !/^application\/json(?:;|$)/i.test(response.headers.get("Content-Type") || "")) {
      throw new GatewayError(502, "Шлюз не отдал исходную загрузку");
    }
    const declared = response.headers.get("Content-Length");
    if (declared !== null && (!/^\d+$/.test(declared) || Number(declared) !== ref.bytes)) {
      throw new GatewayError(502, "Размер загрузки не совпал");
    }
    if (!response.body) throw new GatewayError(502, "Шлюз отдал пустую загрузку");
    reader = response.body.getReader();
    const data = new Uint8Array(ref.bytes);
    let length = 0;
    while (true) {
      const part = await reader.read();
      if (part.done) break;
      if (length + part.value.byteLength > ref.bytes) throw new GatewayError(502, "Загрузка превысила заявленный размер");
      data.set(part.value, length);
      length += part.value.byteLength;
    }
    if (length !== ref.bytes) throw new GatewayError(502, "Загрузка получена не полностью");
    const digest = Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", data)))
      .map(x => x.toString(16).padStart(2, "0")).join("");
    if (digest !== ref.sha256) throw new GatewayError(502, "Контрольная сумма загрузки не совпала");
    let body;
    try { body = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(data)); }
    catch (_) { throw new GatewayError(400, "Bad JSON", digest); }
    if (body && Object.prototype.hasOwnProperty.call(body, "__gateway_upload")) {
      throw new GatewayError(400, "Вложенная обратная загрузка запрещена", digest);
    }
    return { body, sha256: digest };
  } catch (error) {
    if (error instanceof GatewayError) throw error;
    throw new GatewayError(502, "Не удалось скачать загрузку со шлюза");
  } finally {
    clearTimeout(timer);
    controller.abort();
    if (reader) {
      try { await reader.cancel(); } catch (_) {}
    }
  }
}
