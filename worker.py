"""
Bluesky Strike Monitor - Mistral Workflow worker.

Chaque execution:
  1. recupere les posts des dernières N heures (24 par defaut) d'une liste
     de comptes Bluesky (OSINT / presse ukrainienne et internationale),
  2. filtre ceux qui mentionnent des frappes de drones/missiles ukrainiens
     sur le territoire russe,
  3. renvoie un rapport structure (JSON + Markdown) des cibles citees.

Aucune cle API Bluesky n'est necessaire : l'API publique
(public.api.bsky.app) suffit pour les comptes publics.

Fichiers du depot :
  - worker.py          (ce fichier)
  - requirements.txt
  - Dockerfile
"""

import asyncio
from datetime import datetime, timedelta
from typing import Any

import mistralai.workflows as workflows

# NOTE : ne PAS importer httpx/pydantic ici (niveau module) :
# le bac a sable de validation Temporal des workflows bloque les
# imports tiers au niveau du module. Les importer dans les activites.

# ---------------------------------------------------------------------------
# Comptes Bluesky a surveiller
# ---------------------------------------------------------------------------

BSKY_ACCOUNTS: list[str] = [
    "christopherjm.ft.com",
    "perevaa.bsky.social",
    "osint-mirrors.bsky.social",
    "osintradar.bsky.social",
    "darthputinkgb.bsky.social",
    "the-hedgehog.bsky.social",
    "hoholenko.bsky.social",
    "amplifyukraine.eu",
    "agoratlas.com",
    "dallas-ua.bsky.social",
    "dronebomber.bsky.social",
    "ukrainewarpod.bsky.social",
    "kooiker.bsky.social",
    "meanwhileua.bsky.social",
    "mina-hanse.bsky.social",
    "igorsushko.bsky.social",
    "kateinkharkiv.bsky.social",
    "michaelbociurkiw.bsky.social",
    "ralexdc.bsky.social",
    "volberg.bsky.social",
    "szabolcspanyi.bsky.social",
    "istories.bsky.social",
    "delfoo.bsky.social",
    "sonarrow.bsky.social",
    "starskyua.bsky.social",
    "lxsummer1.bsky.social",
    "marqs.bsky.social",
    "avalaina.bsky.social",
    "mrsoroka.bsky.social",
    "hannaliubakova.bsky.social",
    "myroslavapetsa.bsky.social",
    "nataliyavasilyeva.bsky.social",
    "siobhanogrady.bsky.social",
    "msnegovaya.bsky.social",
    "julianroepcke.bsky.social",
    "vkaramurza.bsky.social",
    "massdara.bsky.social",
    "golub.bsky.social",
    "mariiazolk.bsky.social",
    "pjotrsauer.bsky.social",
    "mylovanov.bsky.social",
    "allapoedie.bsky.social",
    "stanislaspoyet.bsky.social",
    "meduzaproject.bsky.social",
    "slugovskyy.bsky.social",
    "evgen-istrebin.bsky.social",
    "fabricedeprez.bsky.social",
    "ukrainemap.bsky.social",
    "michaelmackay.bsky.social",
    "antibot4navalny.bsky.social",
    "dobrokhotov.bsky.social",
    "osintukraine.com",
    "militarynewsua.bsky.social",
    "florencegaub.bsky.social",
    "pravda.ua",
    "jseldin-news.bsky.social",
    "mclark.bsky.social",
    "katstepanenko.bsky.social",
    "karolinahird.bsky.social",
    "georgewbarros.bsky.social",
    "chrisdyork.bsky.social",
    "matisaksk.bsky.social",
    "jjhelin.bsky.social",
    "friendlykozak.bsky.social",
    "sambendett.bsky.social",
    "olgs7.bsky.social",
    "drjademcglynn.bsky.social",
    "mollymckew.bsky.social",
    "ioponomarenko.bsky.social",
    "casusbelli.bsky.social",
    "antongerashchenko.bsky.social",
    "dmytrokuleba.bsky.social",
    "shaunwalker7.bsky.social",
    "kajakallas.bsky.social",
    "texty.org",
    "benborges.xyz",
    "theins.press",
    "nicolashenin.net",
    "emgryn.bsky.social",
    "xtytelman.bsky.social",
    "christogrozev.bsky.social",
    "bctallis.bsky.social",
    "garrykasparov.bsky.social",
    "euromaidanpress.bsky.social",
    "alexandrehrn.bsky.social",
    "alexbondodua.bsky.social",
    "enmemorial.bsky.social",
    "whereisrussia.today",
    "munitionsportal.bsky.social",
    "frog-of-war.bsky.social",
    "nicolastenzer.bsky.social",
    "nicolasquenel.bsky.social",
    "irynavoichuk.bsky.social",
    "heliosrunner.bsky.social",
    "jjsmart.bsky.social",
    "oksanamarkarova.bsky.social",
    "vzhora.bsky.social",
    "mynameredacted.bsky.social",
    "warmonitor3.bsky.social",
    "cyrilleamoursky.bsky.social",
    "christophetymo.bsky.social",
    "abdujalil.bsky.social",
    "evanhill.bsky.social",
    "stratcomcentre.bsky.social",
    "topcargo200.com",
    "uacontrolmap.bsky.social",
    "bradyafr.bsky.social",
    "blackbirdgroup.bsky.social",
    "nlwartracker.bsky.social",
    "mykolabielieskov.bsky.social",
    "holgerroonemaa.bsky.social",
    "michaeldweiss.bsky.social",
    "glushko.bsky.social",
    "chuckpfarrer.bsky.social",
    "okhromeychuk.bsky.social",
    "tendar.bsky.social",
    "ragnars.bsky.social",
    "warnoir.bsky.social",
    "loupbureau.bsky.social",
    "johnsweeneyroar.bsky.social",
    "kylejglen.bsky.social",
    "eydouxt.bsky.social",
    "ruthdeyermond.bsky.social",
    "golovchenko.bsky.social",
    "intelcrab.bsky.social",
    "jaybeecher.bsky.social",
    "rjlee.bsky.social",
    "emilkastehelmi.bsky.social",
    "sebastienroblin.bsky.social",
    "avaitovich.bsky.social",
    "ikoshiw.bsky.social",
    "fscarr.bsky.social",
    "boomych.bsky.social",
    "steffanwatkins.bsky.social",
    "paulmcleary.bsky.social",
    "ian-j-stewart.bsky.social",
    "euanmacdonald.bsky.social",
    "andersostlund.bsky.social",
    "jimsciutto.bsky.social",
    "olliecarroll.bsky.social",
    "gbrumfiel.bsky.social",
    "hoanssolo.bsky.social",
    "alexandregagne.bsky.social",
    "aeowinpact.bsky.social",
    "azovsouth.bsky.social",
    "arturrehi.bsky.social",
    "specialkhersoncat.bsky.social",
    "tatarigami.bsky.social",
    "thelookout.bsky.social",
    "jayinkyiv.bsky.social",
    "praisethesteph.bsky.social",
    "emmanuellechaze.com",
    "jimmyrushton.bsky.social",
    "benoitmgh.bsky.social",
    "ericlecomte.bsky.social",
    "amirintel.bsky.social",
    "aidefranceukr.bsky.social",
    "bbcstever.bsky.social",
    "konradmuzyka.bsky.social",
    "phillipspobrien.bsky.social",
    "tarasmi.bsky.social",
    "realcynicalfox.bsky.social",
    "markhertling.bsky.social",
    "ldfreedman.bsky.social",
    "danlamothe.bsky.social",
    "shashj.bsky.social",
    "warinthefuture.bsky.social",
    "maxseddon.bsky.social",
    "noclador.bsky.social",
    "mancer.bsky.social",
    "bendobrown.bsky.social",
    "timkmak.bsky.social",
    "russianforces.org",
    "yarotrof.bsky.social",
    "dkaleniuk.bsky.social",
    "abarbashin.bsky.social",
    "khodorkovsky.com",
    "2tswfl.bsky.social",
    "alexkhrebet.bsky.social",
    "stefsiohan.bsky.social",
    "dmitri.silverado.org",
    "alexjubelin.bsky.social",
    "goyamichel.bsky.social",
    "colinlebedev.cpesr.fr",
    "elintnews.bsky.social",
    "rebeccarambar.bsky.social",
    "coupsure.bsky.social",
    "olgalautman.bsky.social",
    "glasnostgone.bsky.social",
    "wartranslated.bsky.social",
    "warmapper.org",
    "ukraineworld.bsky.social",
    "killedinukraine.bsky.social",
    "ukraineaidops.org",
    "united24media.com",
    "uaweapons.bsky.social",
    "deaidua.org",
    "osinttechnical.bsky.social",
    "vatniksoup.bsky.social",
    "ozkaterji.bsky.social",
    "kevinrothrock.me",
    "rebel44cz.bsky.social",
    "juliadavisnews.bsky.social",
    "kyivindependent.com",
    "vcdgf555.bsky.social",
    "covertshores.bsky.social",
    "thestudyofwar.bsky.social",
    "noelreports.com",
]

# ---------------------------------------------------------------------------
# Filtre : mots-cles lies a des frappes ukrainiennes sur le territoire russe
# (anglais, ukrainien, russe - en minuscules, comparaison insensible a la casse)
# ---------------------------------------------------------------------------

STRIKE_KEYWORDS: list[str] = [
    # anglais
    "drone", "missile", "strike", "struck", "hit", "attack", "overnight",
    "refinery", "airfield", "air base", "depot", "oil terminal", "plant",
    # ukrainien
    "дрон", "безпілотник", "ракета", "удар", "атака", "влучан",
    "ніч", "рафінерія", "аеродром", "склад", "завод",
    # russe
    "дрон", "бпла", "ракета", "удар", "поражен", "атака", "ноч",
    "нпз", "рафинерия", "аэродром", "склад", "завод",
    # allemand (certains comptes francophones/germanophones)
    "drohne", "rakete", "raffinerie",
]

# Mots-cles qui signalent un contexte "cible en Russie"
RUSSIA_CONTEXT_KEYWORDS: list[str] = [
    "russia", "russian", "moscow", "belgorod", "kursk", "voronezh", "rostov",
    "nizhny novgorod", "kstovo", "saratov", "samara", "smolensk", "bryansk",
    "oryol", "tula", "novorossiysk", "krai", "oblast", " Crimea".strip(),
    "россия", "москва", "белгород", "курск", "воронеж", "ростов",
    "нижний новгород", "кстово", "саратов", "самара", "смоленск", "брянск",
    "орел", "тула",
]

PUBLIC_API = "https://public.api.bsky.app/xrpc"
MAX_PAGES_PER_ACCOUNT = 3
PAGE_LIMIT = 100
CONCURRENCY = 10


def _matches_filter(text: str) -> bool:
    t = text.lower()
    has_strike = any(k in t for k in STRIKE_KEYWORDS)
    has_russia = any(k in t for k in RUSSIA_CONTEXT_KEYWORDS)
    return has_strike and has_russia


# ---------------------------------------------------------------------------
# Entrees / sorties du workflow
# (fenetre fixe de 24 h - pas de parametre d'entree requis)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Activite 1 : collecte des posts Bluesky
# ---------------------------------------------------------------------------

@workflows.activity(
    start_to_close_timeout=timedelta(minutes=15),
    retry_policy_max_attempts=2,
)
async def fetch_matching_posts(since_iso: str) -> dict[str, Any]:
    """Recupere les posts recents de tous les comptes et retourne ceux
    qui correspondent au filtre 'frappe en Russie'."""
    import httpx  # import local : les activites ne sont pas sandboxees
    since = datetime.fromisoformat(since_iso.replace("Z", "+00:00"))
    matching: list[dict[str, Any]] = []
    unreachable: list[str] = []
    stats = {"accounts_total": len(BSKY_ACCOUNTS), "accounts_ok": 0,
             "accounts_unreachable": 0, "posts_scanned": 0}

    sem = asyncio.Semaphore(CONCURRENCY)

    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        async def fetch_account(actor: str) -> None:
            async with sem:
                cursor = None
                scanned = 0
                found = 0
                try:
                    for _ in range(MAX_PAGES_PER_ACCOUNT):
                        params: dict[str, Any] = {"actor": actor, "limit": PAGE_LIMIT}
                        if cursor:
                            params["cursor"] = cursor
                        resp = await client.get(
                            f"{PUBLIC_API}/app.bsky.feed.getAuthorFeed", params=params
                        )
                        if resp.status_code in (400, 404, 410):
                            # compte inexistant ou supprime
                            unreachable.append(actor)
                            return
                        resp.raise_for_status()
                        data = resp.json()
                        for item in data.get("feed", []):
                            post = item.get("post", {})
                            indexed_at = post.get("indexedAt", "")
                            try:
                                ts = datetime.fromisoformat(
                                    indexed_at.replace("Z", "+00:00")
                                )
                            except ValueError:
                                continue
                            if ts < since:
                                continue
                            record = post.get("record", {})
                            text = record.get("text", "")
                            scanned += 1
                            if _matches_filter(text):
                                found += 1
                                matching.append({
                                    "author": post.get("author", {}).get("handle", actor),
                                    "posted_at": record.get("createdAt", indexed_at),
                                    "url": post.get("uri", ""),
                                    "https_url": _post_url(post.get("uri", "")),
                                    "text": text,
                                })
                        cursor = data.get("cursor")
                        if not cursor:
                            break
                    stats["accounts_ok"] += 1
                    stats["posts_scanned"] += scanned
                except Exception:
                    unreachable.append(actor)
                    stats["accounts_unreachable"] += 1

        await asyncio.gather(*(fetch_account(a) for a in BSKY_ACCOUNTS))

    matching.sort(key=lambda p: p["posted_at"], reverse=True)
    stats["accounts_unreachable_list"] = sorted(set(unreachable))
    return {"stats": stats, "posts": matching}


def _post_url(uri: str) -> str:
    """at://did:plc:xxx/app.bsky.feed.post/yyy -> lien web."""
    try:
        did, _, rkey = uri.replace("at://", "").rpartition("/app.bsky.feed.post/")
        return f"https://bsky.app/profile/{did}/post/{rkey}" if rkey else ""
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# Activite 2 : compilation du rapport
# ---------------------------------------------------------------------------

@workflows.activity(start_to_close_timeout=timedelta(minutes=2))
async def compile_report(collected: dict[str, Any], since_iso: str, run_iso: str) -> dict[str, Any]:
    """Compile les posts en rapport Markdown lisible (sans LLM)."""
    posts = collected["posts"]
    stats = collected["stats"]
    lines = [
        f"# Frappes ukrainiennes sur le territoire russe - rapport du {run_iso}",
        "",
        f"Fenetre : posts depuis {since_iso} (dernieres 24 h).",
        f"Comptes scannes : {stats['accounts_total']} "
        f"({stats['accounts_ok']} OK, {stats['accounts_unreachable']} inaccessibles), "
        f"{stats['posts_scanned']} posts recents scannes, "
        f"{len(posts)} posts retenus par le filtre.",
        "",
    ]
    if stats.get("accounts_unreachable_list"):
        lines.append("Comptes inaccessibles : "
                     + ", ".join(stats["accounts_unreachable_list"]))
        lines.append("")
    if not posts:
        lines.append("_Aucun post correspondant au filtre n'a ete trouve._")
    for p in posts:
        lines += [
            f"## @{p['author']} - {p['posted_at']}",
            p["text"],
            f"Lien : {p['https_url'] or p['url']}",
            "",
        ]
    return {"markdown": "\n".join(lines), "posts": posts, "stats": stats}


# ---------------------------------------------------------------------------
# Workflow
# ---------------------------------------------------------------------------

@workflows.workflow.define(
    name="bluesky_strike_monitor",
    workflow_display_name="Bluesky Strike Monitor",
    workflow_description="Rapport quotidien des posts Bluesky sur les frappes ukrainiennes en Russie.",
)
class BlueskyStrikeMonitor:
    @workflows.workflow.entrypoint
    async def run(self) -> str:
        lookback = 24
        now = workflows.workflow.now()
        since = now - timedelta(hours=lookback)
        since_iso = since.isoformat()
        run_iso = now.isoformat()

        collected = await fetch_matching_posts(since_iso)
        report = await compile_report(collected, since_iso, run_iso)
        return report["markdown"]


# ---------------------------------------------------------------------------
# Point d'entree du worker
# ---------------------------------------------------------------------------

async def main() -> None:
    await workflows.run_worker([BlueskyStrikeMonitor])


if __name__ == "__main__":
    asyncio.run(main())
