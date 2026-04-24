 export default {
   async fetch(request) {
    const B2_ACCOUNT_ID = "529665795";
    const B2_KEY_ID = "005d01d287e45580000000001";
    const B2_APP_KEY = "K00503xO0gW8T+ZL1v3ylTj";
    const B2_BUCKET_NAME = "529665795";
    const B2_F0_HOST = "f004.backblazeb2.com";

    let cachedAuthToken = null;
    let tokenExpireTime = 0;

    async function getB2AuthToken() {
      if (cachedAuthToken && Date.now() < tokenExpireTime) {
        return cachedAuthToken;
      }
      const auth = btoa(`${B2_KEY_ID}:${B2_APP_KEY}`);
      const resp = await fetch(`https://api.backblazeb2.com/b2api/v2/b2_authorize_account`, {
        headers: { Authorization: `Basic ${auth}` }
      });
      const data = await resp.json();
      if (!resp.ok) throw new Error("B2授权失败，请检查密钥");
      cachedAuthToken = data.authorizationToken;
      tokenExpireTime = Date.now() + 6 * 24 * 60 * 60 * 1000;
      return cachedAuthToken;
    }

    const url = new URL(request.url);
    const filePath = url.pathname.slice(1);
    if (!filePath) {
      return new Response("请在地址后加文件路径，如 /test.jpg", { status: 400 });
    }

    try {
      const token = await getB2AuthToken();
      const b2FileUrl = `https://${B2_F0_HOST}/file/${B2_BUCKET_NAME}/${filePath}?Authorization=${token}`;
      const b2Resp = await fetch(b2FileUrl);
      if (!b2Resp.ok) throw new Error(`B2返回错误码: ${b2Resp.status}`);
      return new Response(b2Resp.body, {
        headers: {
          "Content-Type": b2Resp.headers.get("Content-Type") || "application/octet-stream",
          "Cache-Control": "public, max-age=86400"
        }
      });
    } catch (err) {
      return new Response(`错误: ${err.message}`, { status: 500 });
    }
  }
};
