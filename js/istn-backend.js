/* I Spent the Night: backend adapter.
   Gives the page the same window.claude.use("db" | "user" | "assets") calls it had on claude.ai,
   backed by Supabase (Postgres table `docs`, Storage bucket `media`, email magic-link login for the crew). */
(() => {
  const SB_URL = "https://zppwfpgsqwwljvgvtnri.supabase.co";
  const SB_KEY = "sb_publishable_sOtBdkzyH6yjLMMuHpB3Bg_ef2iQzLE";
  const BUCKET = "media", DIR = "_blob";
  const MAX_BYTES = 1024 * 1024 * 1024;   // Supabase free plan storage

  const sb = window.supabase.createClient(SB_URL, SB_KEY, { auth: { flowType: "pkce", detectSessionInUrl: true, persistSession: true } });
  let session = null, role = { crew: false, owner: false, name: null };

  const ready = (async () => {
    try {
      const { data } = await sb.auth.getSession();
      session = data.session;
      // clean the ?code= left by the login link
      if (/[?&]code=/.test(location.search)) history.replaceState(null, "", location.pathname + (location.hash || ""));
      if (session) {
        const r = await sb.rpc("my_role");
        if (!r.error && r.data) role = r.data;
        if (role.crew) sb.from("profiles").upsert({ id: session.user.id, name: role.name || (session.user.email || "").split("@")[0] }).then(() => {}, () => {});
      }
    } catch (e) { console.warn("login check failed", e); }
  })();

  /* ---------------- db ---------------- */
  const rid = (n) => { const a = new Uint8Array(n); crypto.getRandomValues(a); return Array.from(a, b => "abcdefghijklmnopqrstuvwxyz0123456789"[b % 36]).join(""); };
  const fail = (error) => { const e = new Error(error.message || "db error"); e.code = error.code === "42501" ? "permission_denied" : (error.code || "db_error"); return e; };
  const snapDoc = (id, data) => ({ id, exists: data != null, data: () => (data == null ? undefined : JSON.parse(JSON.stringify(data))) });

  // one realtime channel per collection; listeners refetch on any change
  const chans = new Map();
  function watch(col, fn) {
    let c = chans.get(col);
    if (!c) {
      c = { fns: new Set(), ch: null };
      c.ch = sb.channel("docs-" + col)
        .on("postgres_changes", { event: "*", schema: "public", table: "docs", filter: "col=eq." + col }, (p) => { c.fns.forEach(f => f(p)); })
        .subscribe();
      chans.set(col, c);
    }
    c.fns.add(fn);
    return () => c.fns.delete(fn);
  }
  const debounce = (fn, ms) => { let t = null; return () => { clearTimeout(t); t = setTimeout(fn, ms); }; };
  // safety net in case a realtime message is missed (phone sleeping, flaky venue wifi)
  const pollers = new Set();
  setInterval(() => { if (!document.hidden) pollers.forEach(f => f()); }, 30000);
  document.addEventListener("visibilitychange", () => { if (!document.hidden) pollers.forEach(f => f()); });

  function docRef(col, id) {
    return {
      id,
      async get() { const { data, error } = await sb.from("docs").select("data").eq("col", col).eq("id", id).maybeSingle(); if (error) throw fail(error); return snapDoc(id, data ? data.data : null); },
      async set(v) { const { error } = await sb.from("docs").upsert({ col, id, data: v, updated_at: new Date().toISOString() }); if (error) throw fail(error); },
      async update(patch) {
        const { data, error } = await sb.rpc("doc_update", { p_col: col, p_id: id, p_patch: patch });
        if (error) throw fail(error);
        if (!data) { const e = new Error("no such document"); e.code = "not_found"; throw e; }
      },
      async delete() { const { error } = await sb.from("docs").delete().eq("col", col).eq("id", id); if (error) throw fail(error); },
      onSnapshot(cb, onErr) {
        let alive = true;
        const load = async () => { try { const s = await this.get(); if (alive) cb(s); } catch (e) { if (alive && onErr) onErr(e); } };
        const again = debounce(load, 150);
        const un = watch(col, (p) => { const row = p.new && p.new.id ? p.new : p.old; if (!row || !row.id || row.id === id) again(); });
        pollers.add(load); load();
        return () => { alive = false; un(); pollers.delete(load); };
      }
    };
  }

  function query(col, ord, lim) {
    const run = async () => {
      // ordering happens here so it works on any field inside the JSON data
      const { data, error } = await sb.from("docs").select("id,data").eq("col", col).limit(5000);
      if (error) throw fail(error);
      let rows = (data || []).map(r => ({ id: r.id, d: r.data || {} }));
      if (ord) {
        const [f, dir] = ord, k = dir === "desc" ? -1 : 1;
        rows.sort((a, b) => { const x = a.d[f], y = b.d[f]; return (x == null) - (y == null) || (x < y ? -k : x > y ? k : 0); });
      }
      if (lim) rows = rows.slice(0, lim);
      return { docs: rows.map(r => snapDoc(r.id, r.d)), size: rows.length, empty: !rows.length };
    };
    return {
      orderBy(f, dir) { return query(col, [f, dir || "asc"], lim); },
      limit(n) { return query(col, ord, n); },
      get: run,
      onSnapshot(cb, onErr) {
        let alive = true;
        const load = async () => { try { const s = await run(); if (alive) cb(s); } catch (e) { if (alive && onErr) onErr(e); } };
        const again = debounce(load, 150);
        const un = watch(col, again);
        pollers.add(load); load();
        return () => { alive = false; un(); pollers.delete(load); };
      }
    };
  }

  const db = {
    collection(col) {
      const q = query(col);
      return {
        ...q,
        doc(id) { return docRef(col, id || rid(20)); },
        async add(v) { const id = rid(20); const { error } = await sb.from("docs").insert({ col, id, data: v }); if (error) throw fail(error); return { id }; }
      };
    },
    doc(path) { const [col, id] = String(path).split("/"); return docRef(col, id); }
  };

  /* ---------------- user ---------------- */
  const user = {
    async canEdit() { return !!role.crew; },
    async can() { return !!role.crew; },
    async isOwner() { return !!role.owner; },
    async id() { return session ? session.user.id : null; },
    async profiles(ids) {
      const out = {};
      if (!role.crew || !ids || !ids.length) return out;
      const { data } = await sb.from("profiles").select("id,name").in("id", ids);
      (data || []).forEach(p => { out[p.id] = { name: p.name || "crew" }; });
      return out;
    }
  };

  /* ---------------- assets ---------------- */
  const hex = () => { const a = new Uint8Array(16); crypto.getRandomValues(a); return Array.from(a, b => b.toString(16).padStart(2, "0")).join(""); };
  const pubUrl = (id) => "/" + DIR + "/" + id;   // served by the site (vercel.json rewrites to storage)
  const assets = {
    async upload(file) {
      if (!role.crew) throw { code: "not_granted" };
      if (file.size > 50 * 1024 * 1024) throw { code: "too_large" };
      const id = hex(), type = file.type || "application/octet-stream";
      const { error } = await sb.storage.from(BUCKET).upload(DIR + "/" + id, file, { contentType: type, cacheControl: "31536000", upsert: false });
      if (error) {
        const m = String(error.message || "").toLowerCase();
        throw { code: /size|large|exceed/.test(m) ? "too_large" : /quota|storage limit/.test(m) ? "quota_exceeded" : /mime|type/.test(m) ? "invalid_request" : "upload_failed", message: error.message };
      }
      return { id, contentType: type, url: pubUrl(id), sizeBytes: file.size };
    },
    async delete(id) { const { error } = await sb.storage.from(BUCKET).remove([DIR + "/" + id]); if (error) throw error; },
    async list() {
      let all = [], off = 0;
      for (;;) {
        const { data, error } = await sb.storage.from(BUCKET).list(DIR, { limit: 1000, offset: off });
        if (error) throw error;
        all = all.concat((data || []).filter(o => o.id));
        if (!data || data.length < 1000) break;
        off += 1000;
      }
      const files = all.map(o => ({ id: o.name, contentType: (o.metadata && o.metadata.mimetype) || "", sizeBytes: (o.metadata && o.metadata.size) || 0, url: pubUrl(o.name) }));
      return { assets: files, usage: { bytes: files.reduce((n, f) => n + f.sizeBytes, 0), maxBytes: MAX_BYTES, files: files.length, maxFiles: 5000 } };
    }
  };

  window.claude = {
    async use(name) {
      await ready;
      if (name === "db") return db;
      if (name === "user") return user;
      if (name === "assets") return role.crew ? assets : null;
      return null;
    }
  };

  /* ---------------- crew login: open the site with #login ---------------- */
  const css = `
  .istn-login{position:fixed;inset:0;z-index:99999;display:grid;place-items:center;background:rgba(40,40,90,.55);padding:16px}
  .istn-login .w{width:min(360px,100%);background:#ddd;border:1px solid #000;box-shadow:3px 3px 0 rgba(0,0,0,.55);font:14px/1.4 Geneva,Verdana,sans-serif;color:#000}
  .istn-login .tb{display:flex;align-items:center;gap:8px;height:22px;padding:0 6px;border-bottom:1px solid #000;background:repeating-linear-gradient(#ddd 0 1px,#888 1px 2px,#ddd 2px 3px)}
  .istn-login .tb i{width:11px;height:11px;border:1px solid #000;background:linear-gradient(135deg,#fff,#888);cursor:pointer}
  .istn-login .tb b{flex:1;text-align:center}.istn-login .tb b span{background:#ddd;padding:0 8px;font:700 13px "Pixelify Sans",monospace}
  .istn-login .bd{margin:8px;background:#fff;border:1px solid #888;padding:14px}
  .istn-login label{display:block;font:12px "Pixelify Sans",monospace;margin-bottom:4px}
  .istn-login input{width:100%;box-sizing:border-box;font:16px Geneva,Verdana,sans-serif;padding:6px;border:1px solid #000}
  .istn-login .row{display:flex;gap:8px;justify-content:flex-end;margin-top:12px}
  .istn-login button{font:13px "Pixelify Sans",monospace;padding:4px 14px;border:1px solid #000;border-radius:4px;background:linear-gradient(#fff,#ccc);cursor:pointer}
  .istn-login button.def{box-shadow:0 0 0 2px #000}
  .istn-login .msg{margin-top:10px;font:12px "Pixelify Sans",monospace;min-height:1.4em}`;
  function loginBox() {
    if (document.querySelector(".istn-login")) return;
    const st = document.createElement("style"); st.textContent = css; document.head.append(st);
    const o = document.createElement("div"); o.className = "istn-login";
    const signedIn = !!session;
    o.innerHTML = `<div class="w" role="dialog" aria-modal="true" aria-labelledby="istnLT"><div class="tb"><i title="Close"></i><b><span id="istnLT">Crew Login</span></b></div><div class="bd"></div></div>`;
    const bd = o.querySelector(".bd");
    const close = () => { o.remove(); if (location.hash === "#login") location.hash = "#all"; };
    o.querySelector(".tb i").onclick = close;
    o.addEventListener("keydown", (e) => { if (e.key === "Escape") close(); });
    if (signedIn) {
      bd.innerHTML = `<p style="margin:0">Signed in as <b></b>.${role.crew ? "" : "<br>This email isn't on the crew list yet."}</p><div class="row"><button type="button" class="out">Log Out</button><button type="button" class="def ok">OK</button></div>`;
      bd.querySelector("b").textContent = session.user.email || "";
      bd.querySelector(".ok").onclick = close;
      bd.querySelector(".out").onclick = async () => { await sb.auth.signOut(); location.replace(location.pathname + "#all"); location.reload(); };
    } else {
      bd.innerHTML = `<form><label for="istnEm">Crew email</label><input id="istnEm" type="email" autocomplete="email" required><div class="row"><button type="button" class="cn">Cancel</button><button type="submit" class="def">Send Link</button></div><div class="msg" role="status"></div></form>`;
      const f = bd.querySelector("form"), msg = bd.querySelector(".msg");
      bd.querySelector(".cn").onclick = close;
      f.onsubmit = async (e) => {
        e.preventDefault();
        const em = f.querySelector("input").value.trim();
        msg.textContent = "Sending…";
        const { error } = await sb.auth.signInWithOtp({ email: em, options: { emailRedirectTo: location.origin + "/", shouldCreateUser: true } });
        msg.textContent = error ? ("Couldn't send: " + error.message) : "Check your email and tap the link on this device.";
      };
    }
    document.body.append(o);
    const inp = o.querySelector("input, .ok"); if (inp) inp.focus();
  }
  const check = () => { if (location.hash === "#login") ready.then(loginBox); };
  window.addEventListener("hashchange", check);
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", check); else check();
})();
