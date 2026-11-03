"""
Russia Monitor v2.0 - Mistral Workflow worker.

Evolution du Bluesky Strike Monitor (v1) : integre, en plus des frappes
ukrainiennes sur le territoire russe, les sujets connexes de la situation
economique et budgetaire de la Russie (debats et votes budgetaires a la
Douma, emissions et echecs de placement d'obligations du Tresor (OFZ),
taux directeur de la Banque centrale, inflation, rouble, sanctions,
recrutement militaire force / mobilisation, economie de guerre).

Chaque execution :
  1. recupere les posts des dernieres 24 heures d'une liste de comptes
     Bluesky (OSINT / presse ukrainienne et internationale),
  1bis. recupere par RSS les articles recents (48 h) de presse economique
     et financiere couvrant la Russie (budget, banque centrale, rouble,
     sanctions, petrole), en excluant les sources de la liste noire,
  2. classe chaque post par THEME : "strikes" (frappe ukrainienne sur le
     territoire russe / territoires occupes) ou "economy" (economie,
     budget, mobilisation). Un post qui parle des deux est classe
     "strikes" en priorite,
  3. agrege les posts de CHAQUE theme par EVENEMENT via un modele Mistral
     (map-reduce) : memes regles qu'en v1 - contradictions signalees
     (quel compte dit quoi), recence ponderee (un post plus recent,
     surtout s'il corrige des informations erronees, est considere comme
     plus probablement exact). Pour le theme "economy" uniquement, les
     articles de presse sont fournis au modele comme contexte
     d'enrichissement des resumes (un article ne cree jamais un evenement
     par lui-meme ; les articles utilises sont cites en source),
  4. renvoie un rapport structure (Markdown) en deux sections :
     "Frappes ukrainiennes sur le territoire russe" puis "Economie,
     budget et mobilisation en Russie", chacune classee par nombre de
     posts (decroissant).

L'agregation requiert la variable d'environnement UKRAINE_STRIKE, liee au
secret de workspace contenant une cle API Mistral. En son absence, ou en
cas d'erreur de l'API, la section concernee retombe sur le format
"compte par compte".

Fichiers du depot :
  - worker.py            (v1 - Bluesky Strike Monitor)
  - worker_v2.py         (ce fichier - Russia Monitor v2)
  - dockerfile           (v1)
  - dockerfile_v2        (v2 - lance worker_v2.py)
  - requirements.txt
"""

import asyncio
import json
import os
from datetime import datetime, timedelta
from typing import Any

import mistralai.workflows as workflows

# NOTE : ne PAS importer httpx/pydantic ici (niveau module) :
# le bac a sable de validation Temporal des workflows bloque les
# imports tiers au niveau du module. Les importer dans les activites.

# ---------------------------------------------------------------------------
# Comptes Bluesky a surveiller (identiques a la v1)
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
    "warTranslated.bsky.social",
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
    "kyivpost.com",
]

# ---------------------------------------------------------------------------
# Filtres : mots-cles par theme
# (en minuscules, comparaison insensible a la casse)
# ---------------------------------------------------------------------------

# Theme "strikes" : frappes ukrainiennes sur le territoire russe
STRIKE_KEYWORDS: list[str] = [
    # anglais
    "drone", "missile", "strike", "struck", "hit", "attack", "overnight",
    "refinery", "airfield", "air base", "depot", "oil terminal", "plant",
    # ukrainien
    "дрон", "безпілотник", "ракета", "удар", "атака", "влучан",
    "ніч", "рафінерія", "аеродром", "склад", "завод",
    # russe
    "бпла", "поражен", "ноч",
    "нпз", "рафинерия", "аэродром",
    # allemand (certains comptes francophones/germanophones)
    "drohne", "rakete", "raffinerie",
    # culture Internet / jargon specifique au conflit en Ukraine
    "bavovna", "banderol", "meatwave", "kinetic sanction", "fp-7", "fp-9",
]

# Theme "economy" : situation economique et budgetaire de la Russie
ECONOMY_KEYWORDS: list[str] = [
    # anglais
    "budget", "deficit", "duma", "state duma", "bond", "bonds",
    "obligation", "obligations", "ofz", "treasury", "rouble", "ruble",
    "inflation", "interest rate", "key rate", "central bank", "gdp",
    "national wealth fund", "sanction", "sanctions", "war economy",
    "recruit", "recruitment", "recruiting", "conscription",
    "mobilisation", "mobilization", "pawnshop", "labor shortage",
    "oil revenue", "gas revenue", "price cap",
    # ukrainien
    "бюджет", "дефіцит", "інфляція", "рубль", "мобілізація", "призов",
    # russe
    "бюджет", "дефицит", "облигац", "офз", "рубл", "инфляц",
    "ключевая ставка", "мобилизац", "призыв", "центробанк",
]

# Mots-cles qui signalent un contexte "Russie" (commun aux deux themes)
RUSSIA_CONTEXT_KEYWORDS: list[str] = [
    "russia", "russian", "moscow", "belgorod", "kursk", "voronezh", "rostov",
    "nizhny novgorod", "kstovo", "saratov", "samara", "smolensk", "bryansk",
    "oryol", "tula", "novorossiysk", "krai", "oblast", " Crimea".strip(),
    # jargon du conflit designant la Russie / l'armee russe
    "ruzzia", "putler", "orcs",
    "россия", "москва", "белгород", "курск", "воронеж", "ростов",
    "нижний новгород", "кстово", "саратов", "самара", "смоленск", "брянск",
    "орел", "тула",
]

PUBLIC_API = "https://public.api.bsky.app/xrpc"
MAX_PAGES_PER_ACCOUNT = 3
PAGE_LIMIT = 100
CONCURRENCY = 10

# ---------------------------------------------------------------------------
# Veille presse economique et financiere (contexte d'enrichissement du theme
# "economy" uniquement ; les articles ne creent pas d'evenements par eux-memes)
# ---------------------------------------------------------------------------

# Flux RSS DIRECTS d'editeurs (URLs d'origine, pas d'agrégateur type
# Google News/MSN dont les liens rediriges sont illisibles). bne IntelliNews
# est le specialiste de l'economie russe ; les autres couvrent l'economie
# et la finance internationales, le filtre de pertinence ne garde que
# Russie/Ukraine.
NEWS_FEEDS: list[dict[str, str]] = [
    {"source": "bne IntelliNews", "always_relevant": "1",
     "url": "https://www.intellinews.com/feed/"},
    {"source": "Meduza (en)",
     "url": "https://meduza.io/rss/en/all"},
    {"source": "The Insider (en)",
     "url": "https://theins.press/en/feed"},
    {"source": "Euronews Business",
     "url": "https://www.euronews.com/rss?level=theme&name=business"},
    {"source": "Investing.com",
     "url": "https://www.investing.com/rss/news_25.rss"},
    {"source": "MarketWatch",
     "url": "https://feeds.content.dowjones.io/public/rss/mw_topstories"},
    {"source": "Ukrinform",
     "url": "https://www.ukrinform.net/rss/block-lastnews"},
    {"source": "Euromaidan Press",
     "url": "https://www.euromaidanpress.com/feed/"},
    {"source": "Kyiv Post",
     "url": "https://www.kyivpost.com/feed/"},
]

# Domaines exclus (liste noire personnelle : jamais citer ces sources).
NEWS_BLACKLIST_DOMAINS: list[str] = [
    # medias bases en Russie
    "rt.com", "russian.rt", "tass.ru", "tass.com", "interfax", "sputnik",
    "rbc.ru", "vedomosti", "kommersant", "moscowtimes", "gazeta.ru",
    "lenta.ru", "ria.ru", "1tv", "vgtrk", "rg.ru",
    # medias bases en Chine (y compris Hong Kong)
    "xinhua", "chinadaily", "globaltimes", "scmp.com", "news.cn",
    "caixin", "thepaper.cn",
    # autres sources de la liste noire
    "foxnews", "newsmax", "breitbart",
    # aggregation de blogs peu fiables reperee dans les recherches
    "playnorthjoy",
]

NEWS_MAX_PER_FEED = 12      # articles retenus au maximum par flux
MAX_NEWS_CHARS = 600       # troncature du resume d'un article
NEWS_TIMEOUT_SECONDS = 25

# Un article n'est retenu que si son TITRE mentionne la Russie, l'Ukraine
# ou un mot-cle associe (evite le bruit : Armenie, Pologne, etc.)
NEWS_RELEVANCE_KEYWORDS: list[str] = [
    "russia", "russian", "ruble", "rouble", "kremlin", "moscow", "putin",
    "ukraine", "ukrainian", "kyiv", "zelensky", "ofz", "siluanov",
    "nabiullina", "gazprom", "rosneft", "sanction",
]

# ---------------------------------------------------------------------------
# Parametres de l'agregation par evenement
# ---------------------------------------------------------------------------

AGGREGATION_MODEL = os.environ.get("AGGREGATION_MODEL", "mistral-medium-latest")
BATCH_SIZE = 60            # posts par appel "map"
MAX_POST_CHARS = 600       # troncature du texte d'un post pour l'appel
LLM_TIMEOUT_SECONDS = 240


def _post_theme(text: str) -> str | None:
    """Classe un post par theme : "strikes" (prioritaire), "economy",
    ou None s'il ne correspond a aucun theme. Un post pertinent pour les
    deux themes est classe "strikes"."""
    t = text.lower()
    if not any(k in t for k in RUSSIA_CONTEXT_KEYWORDS):
        return None
    if any(k in t for k in STRIKE_KEYWORDS):
        return "strikes"
    if any(k in t for k in ECONOMY_KEYWORDS):
        return "economy"
    return None


# ---------------------------------------------------------------------------
# Activite 1 : collecte des posts Bluesky
# ---------------------------------------------------------------------------

@workflows.activity(
    start_to_close_timeout=timedelta(minutes=15),
    retry_policy_max_attempts=2,
)
async def fetch_matching_posts(since_iso: str) -> dict[str, Any]:
    """Recupere les posts recents de tous les comptes et retourne ceux
    qui correspondent aux themes 'strikes' ou 'economy', avec leur theme."""
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
                            theme = _post_theme(text)
                            if theme:
                                matching.append({
                                    "author": post.get("author", {}).get("handle", actor),
                                    "posted_at": record.get("createdAt", indexed_at),
                                    "url": post.get("uri", ""),
                                    "https_url": _post_url(post.get("uri", "")),
                                    "text": text,
                                    "theme": theme,
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
# Activite 1bis : veille presse economique et financiere (RSS)
# ---------------------------------------------------------------------------

def _news_is_blacklisted(url: str) -> bool:
    u = (url or "").lower()
    return any(d in u for d in NEWS_BLACKLIST_DOMAINS)


ATOM_NS = "{http://www.w3.org/2005/Atom}"


def _el_text(el: Any, tag: str) -> str:
    """Texte d'un element RSS (sans namespace) ou Atom (namespacé)."""
    return (el.findtext(tag) or el.findtext(ATOM_NS + tag) or "").strip()


def _parse_news_date(raw: str | None) -> datetime | None:
    """Parse une date RSS RFC 822 ('Tue, 03 Nov 2026 13:39:00 GMT') ou
    Atom ISO 8601 ('2026-11-03T14:38:40+00:00'). Retourne None sinon."""
    if not raw:
        return None
    raw = raw.strip()
    try:
        from email.utils import parsedate_to_datetime
        return parsedate_to_datetime(raw)  # gere 'GMT' et '+0000'
    except (TypeError, ValueError):
        pass
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


@workflows.activity(
    start_to_close_timeout=timedelta(minutes=5),
    retry_policy_max_attempts=2,
)
async def fetch_economy_news(since_iso: str) -> dict[str, Any]:
    """Recupere les articles recents (24-48 h) des flux RSS de presse
    economique et financiere, en excluant les sources de la liste noire.

    Retourne {"items": [...], "stats": {...}}. Les articles servent
    uniquement de contexte d'enrichissement du theme "economy".
    """
    import httpx
    import xml.etree.ElementTree as ET

    since = datetime.fromisoformat(since_iso.replace("Z", "+00:00"))
    items: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    feeds_ok = 0
    feeds_failed: list[str] = []

    sem = asyncio.Semaphore(4)

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(NEWS_TIMEOUT_SECONDS),
        headers={"User-Agent": "Mozilla/5.0 (compatible; RussiaMonitor/2.1)"},
        follow_redirects=True,
    ) as client:
        async def fetch_feed(feed: dict[str, str]) -> None:
            nonlocal feeds_ok
            async with sem:
                try:
                    resp = await client.get(feed["url"])
                    resp.raise_for_status()
                    root = ET.fromstring(resp.text)
                except Exception:
                    feeds_failed.append(feed["source"])
                    return
                count = 0
                # Gerer a la fois RSS 2.0 (<item>) et Atom (<entry>)
                nodes = list(root.iter("item")) or list(root.iter(
                    f"{ATOM_NS}entry"))
                for it in nodes:
                    if count >= NEWS_MAX_PER_FEED:
                        break
                    title = _el_text(it, "title")
                    link = (it.findtext("link") or "").strip()
                    if not link:
                        # Atom : <link href="..."/>
                        for le in it.iter(f"{ATOM_NS}link"):
                            link = (le.get("href") or "").strip()
                            if link:
                                break
                    if not title or not link:
                        continue
                    desc = _el_text(it, "description") or _el_text(it, "summary")
                    if (not feed.get("always_relevant")
                            and not any(k in title.lower()
                                        for k in NEWS_RELEVANCE_KEYWORDS)):
                        continue
                    if _news_is_blacklisted(link):
                        continue
                    pub_raw = _el_text(it, "pubDate") or _el_text(it, "published") \
                        or _el_text(it, "updated")
                    pub_dt = _parse_news_date(pub_raw)
                    if pub_dt is None or pub_dt < since:
                        continue
                    if link in seen_urls:
                        continue
                    seen_urls.add(link)
                    items.append({
                        "source": feed["source"],
                        "title": title,
                        "url": link,
                        "published_at": pub_dt.isoformat(),
                        "summary": desc[:MAX_NEWS_CHARS],
                    })
                    count += 1
                feeds_ok += 1

        await asyncio.gather(*(fetch_feed(f) for f in NEWS_FEEDS))

    items.sort(key=lambda i: i["published_at"], reverse=True)
    stats = {"feeds_total": len(NEWS_FEEDS), "feeds_ok": feeds_ok,
             "feeds_failed_list": sorted(set(feeds_failed)),
             "news_items": len(items)}
    return {"items": items, "stats": stats}


def _news_context(news: dict[str, Any] | None) -> str:
    """Formate les articles de presse en contexte numerote [A0], [A1]...
    Chaine vide si aucun article."""
    if not news or not news.get("items"):
        return ""
    lines = ["", "Articles de presse economique et financiere recents "
             "(contexte complementaire, prefixes [A...]) :"]
    for i, a in enumerate(news["items"]):
        desc = a["summary"].replace("\n", " ")
        lines.append(f"[A{i}] ({a['source']}, {a['published_at']}) "
                     f"{a['title']} -- {a['url']}{(' ' + desc) if desc else ''}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Activite 2 : aggregation des posts par evenement (Mistral LLM)
# ---------------------------------------------------------------------------
# Map-reduce par theme : les posts d'un theme sont groupes par lots ; chaque
# lot est analyse pour en extraire les evenements ; puis un appel final
# fusionne les evenements et synthetise chacun, en signalant les
# contradictions entre comptes et en ponderant par la recence des posts.

THEME_MAP_INTROS: dict[str, str] = {
    "strikes": """Tu es un analyste OSINT. Voici une liste numerotee de posts Bluesky (comptes OSINT et de presse couvrant les frappes ukrainiennes sur le territoire russe).

Tache : regroupe les posts qui rapportent ou commentent LE MEME evenement concret de frappe ukrainienne sur le territoire russe ou les territoires occupes (meme cible, meme lieu, meme moment). Un meme evenement peut etre vu par plusieurs comptes ; un post qui corrige une information anterieure appartient au meme evenement.""",
    "economy": """Tu es un analyste economique specialiste de la Russie. Voici une liste numerotee de posts Bluesky (comptes OSINT et de presse couvrant la situation economique russe).

Tache : regroupe les posts qui rapportent ou commentent LE MEME evenement concret de la situation economique et budgetaire de la Russie (annonce ou vote budgetaire a la Douma, emission ou echec de placement d'obligations du Tresor / OFZ, decision de taux de la Banque centrale, inflation, cours du rouble, sanctions economiques et contournement, recrutement militaire force ou mobilisation, penurie de main-d'oeuvre, rentrees petrolieres et gazieres). Un meme evenement peut etre vu par plusieurs comptes ; un post qui corrige une information anterieure appartient au meme evenement.""",
}

THEME_REDUCE_INTROS: dict[str, str] = {
    "strikes": """Tu es un analyste OSINT charge de produire la synthese finale d'un rapport quotidien sur les frappes ukrainiennes sur le territoire russe.""",
    "economy": """Tu es un analyste economique charge de produire la synthese finale d'un rapport quotidien sur la situation economique et budgetaire de la Russie en guerre (budget, dette, obligations du Tresor, politique monetaire, sanctions, mobilisation de main-d'oeuvre).""",
}

THEME_REDUCE_FILTERS: dict[str, str] = {
    "strikes": "3. Ne garde QUE les evenements correspondant a une frappe ukrainienne sur le territoire russe ou directement lies (infrastructure russe ou territories occupes).",
    "economy": "3. Ne garde QUE les evenements relevant de la situation economique, budgetaire, monetaire ou de mobilisation de la Russie (y compris les consequences des frappes sur l'economie russe, comme les restrictions de carburant ou les perturbations de production).",
}

MAP_PROMPT = """{theme_intro}

Pour chaque evenement identifie, retourne :
- "title" : titre court et factuel (en francais),
- "post_ids" : les numeros des posts qui s'y rapportent,
- "hint" : une phrase resume de l'evenement.

Un post ne peut appartenir qu'a un seul evenement (celui qu'il decrit principalement). Un post purement general (pas un evenement concret) doit etre ignore.
{news_context}
Les eventuels articles de presse listes apres les posts ne servent qu'a mieux identifier les evenements : un article n'est PAS un post, ne le classe jamais dans post_ids.

Reponds UNIQUEMENT avec un objet JSON valide de la forme exacte :
{{"events": [{{"title": "...", "post_ids": [0], "hint": "..."}}]}}

Posts :
{posts}"""

REDUCE_PROMPT = """{theme_intro}

Tu disposes d'evenements candidats, extraits de plusieurs lots de posts, avec pour chacun les posts sources (numero, auteur, horodatage, texte).
{news_context}

Taches :
1. Fusionne les evenements candidats qui decrivent en realite le meme evenement (leurs post_ids doivent etre reunis).
2. Pour chaque evenement fusionne, redige (en francais) :
   - "title" : titre court et factuel,
   - "summary" : synthese agregee des informations disponibles (5 a 10 phrases maximum). Donne priorite aux posts les plus RECENTS : ils ont la plus grande probabilite d'etre exacts, surtout s'ils signalent que des informations anterieures etaient erronees. Integre les corrections et les details confirmes par plusieurs comptes. Si des articles de presse (prefixes [A...]) portent sur ce meme evenement, UTILISE-LES pour completer et enrichir le resume (chiffres, contexte, declarations) en indiquant la source dans le texte (ex. "selon bne IntelliNews").
   - "disagreements" : si des comptes se contredisent sur cet evenement, liste les desaccords en precisant QUEL compte dit QUOI (tableau de chaines, chaque entree au format "@compte : affirmation"). Sinon tableau vide.
   - "latest_correction" : si un post recent corrige des informations anterieures erronees, resume la correction en une phrase ; sinon chaine vide.
   - "post_ids" : tous les numeros de posts rattaches a cet evenement.
   - "article_ids" : les numeros des articles de presse (sans le prefixe [A]) reellement utilises dans le resume ; tableau vide sinon.
{theme_filter}

IMPORTANT : les articles de presse ne creent JAMAIS un evenement par eux-memes ; ils servent uniquement a enrichir les evenements identifies a partir des posts. Un evenement sans post_ids est ignore.

Reponds UNIQUEMENT avec un objet JSON valide de la forme exacte :
{{"events": [{{"title": "...", "summary": "...", "disagreements": ["@compte : affirmation"], "latest_correction": "...", "post_ids": [0], "article_ids": []}}]}}

Evenements candidats :
{events}"""


def _compact_post(idx: int, p: dict[str, Any]) -> str:
    text = p["text"].replace("\n", " ")[:MAX_POST_CHARS]
    return f"[{idx}] @{p['author']} ({p['posted_at']}) : {text}"


def _parse_json_content(content: str) -> Any:
    """Extrait le premier objet JSON d'une reponse du modele.

    Tolere le JSON suivi de texte parasite ("Extra data"), les clotures
    de bloc ``` et les accolades imbriquees dans les chaines.
    """
    content = content.strip()
    if content.startswith("```"):
        content = content.split("```")[1]
        if content.startswith("json"):
            content = content[4:]
        content = content.strip()
    start = content.find("{")
    if start == -1:
        raise ValueError("Aucun JSON trouve dans la reponse du modele")
    try:
        # raw_decode : lit le PREMIER objet JSON complet et ignore ce qui suit
        value, _ = json.JSONDecoder().raw_decode(content[start:])
        return value
    except json.JSONDecodeError:
        # Repli : du plus grand segment compris entre accolades
        end = content.rfind("}")
        if end <= start:
            raise ValueError("JSON incomplet dans la reponse du modele")
        return json.loads(content[start:end + 1])


def _get_events(data: Any) -> list[Any]:
    """Extrait la liste d'evenements d'une reponse du modele, de facon
    tolerante : cles avec guillemets parasites, reponse qui est directement
    une liste, valeur "events" elle-meme une chaine JSON, etc."""
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return []
    for key, value in data.items():
        if str(key).strip().strip("\"'") == "events":
            if isinstance(value, str):
                try:
                    value = json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    return []
            return value if isinstance(value, list) else []
    # Dernier recours : unique cle dont la valeur est une liste
    lists = [v for v in data.values() if isinstance(v, list)]
    return lists[0] if len(lists) == 1 else []


@workflows.activity(
    start_to_close_timeout=timedelta(minutes=20),
    retry_policy_max_attempts=2,
)
async def aggregate_events(collected: dict[str, Any], theme: str,
                           news: dict[str, Any] | None = None) -> dict[str, Any]:
    """Regroupe par evenement les posts du theme donne et les synthetise.

    Si theme == "economy" et que "news" contient des articles de presse
    economique, ceux-ci sont injectes comme contexte d'enrichissement
    (le theme "strikes" n'est jamais enrichi par la presse).

    Retourne {"events": [...], "mode": "llm" | "fallback", "error": ...}.
    En cas d'absence de cle API ou d'erreur de l'API, mode="fallback"
    et events=[].
    """
    posts: list[dict[str, Any]] = [p for p in collected["posts"]
                                    if p.get("theme") == theme]
    # MISTRAL_API_KEY est un nom reserve dans les deploiements geres :
    # le secret de workspace (cle API Mistral) est donc lie a la variable
    # UKRAINE_STRIKE, definie par l'utilisateur.
    api_key = (
        os.environ.get("UKRAINE_STRIKE", "")
        or os.environ.get("BSKY_MISTRAL_API_KEY", "")
        or os.environ.get("MISTRAL_API_KEY", "")
    ).strip()
    if not posts:
        return {"events": [], "mode": "llm", "error": None}
    if not api_key:
        return {"events": [], "mode": "fallback",
                "error": "cle API absente (UKRAINE_STRIKE)"}

    import httpx

    # Contexte presse : uniquement pour le theme economy
    news_ctx = _news_context(news) if theme == "economy" else ""
    n_news = len(news["items"]) if (theme == "economy" and news) else 0

    # Client cree sans gestionnaire de contexte : les appels _chat sont
    # awaits dans le bloc try ci-dessous, apres la definition.
    client = httpx.AsyncClient(
        timeout=httpx.Timeout(LLM_TIMEOUT_SECONDS),
        headers={"Authorization": f"Bearer {api_key}",
                 "Content-Type": "application/json"},
    )

    async def _chat(prompt: str) -> str:
        resp = await client.post(
            "https://api.mistral.ai/v1/chat/completions",
            json={
                "model": AGGREGATION_MODEL,
                "temperature": 0.1,
                "response_format": {"type": "json_object"},
                "messages": [{"role": "user", "content": prompt}],
            },
        )
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"] or ""

    map_prompt_tpl = MAP_PROMPT
    reduce_prompt_tpl = REDUCE_PROMPT

    try:
        last_raw = ""
        # ---- Etape "map" : extraction d'evenements par lot de posts ----
        candidate_events: list[dict[str, Any]] = []
        for start in range(0, len(posts), BATCH_SIZE):
            batch = posts[start:start + BATCH_SIZE]
            lines = "\n".join(
                _compact_post(start + i, p) for i, p in enumerate(batch)
            )
            prompt = map_prompt_tpl.format(
                theme_intro=THEME_MAP_INTROS[theme], news_context=news_ctx,
                posts=lines,
            )
            raw = await _chat(prompt)
            last_raw = raw
            data = _parse_json_content(raw)
            for ev in _get_events(data):
                ids = []
                for i in ev.get("post_ids", []):
                    try:
                        pid = int(i)
                    except (TypeError, ValueError):
                        continue
                    if 0 <= pid < len(posts):
                        ids.append(pid)
                if ids:
                    candidate_events.append({
                        "title": str(ev.get("title", ""))[:200],
                        "hint": str(ev.get("hint", ""))[:400],
                        "post_ids": sorted(set(ids)),
                    })

        if not candidate_events:
            return {"events": [], "mode": "fallback",
                    "error": "Aucun evenement extrait par le modele"}

        # ---- Etape "reduce" : fusion et synthese par evenement ----
        events_repr = json.dumps(candidate_events, ensure_ascii=False)
        sources = {i: p for i, p in enumerate(posts)}
        events_repr += "\n\nTexte des posts sources :\n"
        for ev in candidate_events:
            for pid in ev["post_ids"]:
                events_repr += "\n" + _compact_post(pid, sources[pid])

        prompt = reduce_prompt_tpl.format(
            theme_intro=THEME_REDUCE_INTROS[theme],
            theme_filter=THEME_REDUCE_FILTERS[theme],
            news_context=news_ctx,
            events=events_repr,
        )
        raw = await _chat(prompt)
        last_raw = raw
        data = _parse_json_content(raw)
        events: list[dict[str, Any]] = []
        news_items = news["items"] if (theme == "economy" and news) else []
        for ev in _get_events(data):
            ids = sorted(set(
                int(i) for i in ev.get("post_ids", [])
                if str(i).lstrip("-").isdigit() and 0 <= int(i) < len(posts)
            ))
            if not ids:
                continue
            article_ids: list[int] = []
            for a in ev.get("article_ids", []):
                try:
                    aid = int(a)
                except (TypeError, ValueError):
                    continue
                if 0 <= aid < len(news_items) and aid not in article_ids:
                    article_ids.append(aid)
            disagreements = [str(d) for d in ev.get("disagreements", []) if d]
            events.append({
                "title": str(ev.get("title", "") or "")[:200],
                "summary": str(ev.get("summary", "")).strip(),
                "disagreements": disagreements,
                "latest_correction": str(ev.get("latest_correction", "") or ""),
                "post_ids": ids,
                "article_ids": sorted(article_ids),
            })
        # Classement : evenement le plus rapporte en premier
        events.sort(key=lambda e: len(e["post_ids"]), reverse=True)
        return {"events": events, "mode": "llm", "error": None}
    except Exception as exc:  # pragma: no cover - securite de repli
        detail = f"{type(exc).__name__}: {exc}"
        if last_raw:
            detail += f" | extrait du modele : {last_raw[:200]!r}"
        return {"events": [], "mode": "fallback", "error": detail}


# ---------------------------------------------------------------------------
# Activite 3 : compilation du rapport
# ---------------------------------------------------------------------------

@workflows.activity(start_to_close_timeout=timedelta(minutes=2))
async def compile_report(collected: dict[str, Any],
                         aggregations: dict[str, dict[str, Any]],
                         since_iso: str, run_iso: str,
                         lookback: int = 24,
                         news: dict[str, Any] | None = None) -> dict[str, Any]:
    """Compile le rapport Markdown en deux sections : frappes, puis
    economie/budget/mobilisation. Chaque section agregee par evenement
    (classes par nombre de posts), ou compte par compte en cas de repli."""
    posts = collected["posts"]
    stats = collected["stats"]
    posts_by_theme = {
        theme: [p for p in posts if p.get("theme") == theme]
        for theme in ("strikes", "economy")
    }

    lines = [
        f"# Frappes ukrainiennes et situation economique de la Russie - rapport v2 du {run_iso}",
        "",
        f"Fenetre : posts depuis {since_iso} (dernieres {lookback} h).",
        f"Comptes scannes : {stats['accounts_total']} "
        f"({stats['accounts_ok']} OK, {stats['accounts_unreachable']} inaccessibles), "
        f"{stats['posts_scanned']} posts recents scannes, "
        f"{len(posts_by_theme['strikes'])} posts frappes, "
        f"{len(posts_by_theme['economy'])} posts economie retenus par le filtre.",
        "",
    ]
    if stats.get("accounts_unreachable_list"):
        lines.append("Comptes inaccessibles : "
                     + ", ".join(stats["accounts_unreachable_list"]))
        lines.append("")
    news_items = news.get("items", []) if news else []
    news_stats = news.get("stats", {}) if news else {}
    if news_items:
        lines.append(f"Veille presse economique et financiere : "
                     f"{len(news_items)} articles retenus "
                     f"({news_stats.get('feeds_ok', 0)}/{news_stats.get('feeds_total', 0)} flux OK, "
                     f"fenetre 48 h, sources de la liste noire exclues).")
        lines.append("")

    section_titles = {
        "strikes": "Frappes ukrainiennes sur le territoire russe",
        "economy": "Economie, budget et mobilisation en Russie",
    }

    all_events: dict[str, list[dict[str, Any]]] = {}
    for theme in ("strikes", "economy"):
        agg = aggregations.get(theme, {"events": [], "mode": "fallback",
                                       "error": None})
        theme_posts = posts_by_theme[theme]
        events = sorted(agg.get("events", []),
                        key=lambda e: len(e.get("post_ids", [])), reverse=True)
        all_events[theme] = events
        mode = agg.get("mode", "fallback")
        error = agg.get("error")

        lines.append(f"## {section_titles[theme]}")
        lines.append("")
        if mode == "llm" and events:
            lines.append(f"{len(events)} evenements identifies, classes du plus rapporte au moins rapporte.")
            lines.append("")
            for rank, ev in enumerate(events, start=1):
                nb = len(ev["post_ids"])
                lines.append(f"### Evenement {rank} : {ev['title']} ({nb} posts)")
                lines.append("")
                if ev["summary"]:
                    lines.append(ev["summary"])
                    lines.append("")
                if ev.get("latest_correction"):
                    lines.append(f"**Correction recente** : {ev['latest_correction']}")
                    lines.append("")
                if ev.get("disagreements"):
                    lines.append("**Desaccord entre comptes** :")
                    lines.append("")
                    for d in ev["disagreements"]:
                        lines.append(f"- {d}")
                    lines.append("")
                lines.append("**Sources** :")
                lines.append("")
                for pid in ev["post_ids"]:
                    p = theme_posts[pid]
                    lines.append(f"- @{p['author']} - {p['posted_at']} - {p['https_url'] or p['url']}")
                for aid in ev.get("article_ids", []):
                    if 0 <= aid < len(news_items):
                        a = news_items[aid]
                        lines.append(f"- [presse] {a['source']} - {a['title']} - {a['url']}")
                lines.append("")
        else:
            if error:
                lines.append(f"_Agregation par evenement indisponible ({error}) ; format compte par compte._")
                lines.append("")
            if not theme_posts:
                lines.append("_Aucun post correspondant a ce theme n'a ete trouve._")
                lines.append("")
            for p in theme_posts:
                lines += [
                    f"### @{p['author']} - {p['posted_at']}",
                    p["text"],
                    f"Lien : {p['https_url'] or p['url']}",
                    "",
                ]
    return {"markdown": "\n".join(lines), "posts": posts,
            "stats": stats, "events": all_events}


# ---------------------------------------------------------------------------
# Workflow
# ---------------------------------------------------------------------------

@workflows.workflow.define(
    name="bluesky_strike_monitor_v2",
    workflow_display_name="Russia Monitor v2 (Frappes & Economie)",
    workflow_description="Rapport quotidien des posts Bluesky sur les frappes ukrainiennes en Russie et la situation economique et budgetaire russe, agrege par evenement et par theme.",
)
class RussiaMonitorV2:
    @workflows.workflow.entrypoint
    async def run(self) -> str:
        lookback = 24  # fenetre elargie : l'actualite economique est plus lente
        now = workflows.workflow.now()
        since = now - timedelta(hours=lookback)
        since_iso = since.isoformat()
        run_iso = now.isoformat()

        collected = await fetch_matching_posts(since_iso)
        news = await fetch_economy_news(
            (now - timedelta(hours=48)).isoformat()
        )
        agg_strikes = await aggregate_events(collected, "strikes")
        agg_economy = await aggregate_events(collected, "economy", news)
        report = await compile_report(
            collected,
            {"strikes": agg_strikes, "economy": agg_economy},
            since_iso, run_iso, lookback, news,
        )
        return report["markdown"]


# ---------------------------------------------------------------------------
# Point d'entree du worker
# ---------------------------------------------------------------------------

async def main() -> None:
    await workflows.run_worker([RussiaMonitorV2])


if __name__ == "__main__":
    asyncio.run(main())
