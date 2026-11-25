# I Spent the Night · Tour Blog

Lillenskap's tour blog for *I Spent the Night with Brent Faiyaz*: Chicago 11.30 · Brooklyn 12.03 · Los Angeles 12.11.

- `index.html`: the whole site (Mac OS 8 desktop).
- `js/istn-backend.js`: connects the site to Supabase (posts, uploads, crew login).
- `js/supabase-2.99.3.js`: Supabase client library.
- `_blob/`: the site's built-in art (stickers, NUWO, iso, avatar). Uploaded photos and videos live in Supabase Storage and are served from the same `/_blob/<id>` path through `vercel.json`.

Crew login: open the site with `#login` at the end of the address.
Every push to `main` deploys to Vercel.
