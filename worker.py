"""
Bluesky Strike Monitor - Mistral Workflow worker.

Chaque execution:
  1. recupere les posts des dernieres N heures (12 par defaut) d'une liste
     de comptes Bluesky (OSINT / presse ukrainienne et internationale),
  2. filtre ceux qui mentionnent des frappes de drones/missiles ukrainiens
     sur le territoire russe,
  3. agrege les posts par EVENEMENT via un modele Mistral : les posts qui
     rapportent ou commentent le meme evenement sont regroupes, leurs
     informations synthetisees, les contradictions entre comptes sont
     signalees (qui dit quoi), et la recence des posts est prise en compte
     (un post plus recent, surtout s'il corrige des informations erronees,
     est considere comme plus probablement exact),
  4. renvoie un rapport structure (Markdown) ou les evenements sont classes
     par nombre de posts les mentionnant (decroissant).

L'agregation requiert la variable d'environnement UKRAINE_STRIKE, liee au
secret de workspace contenant une cle API Mistral. En son absence, ou en cas
d'erreur de l'API, le workflow retombe sur l'ancien format "compte par compte".

Fichiers du depot :
  - worker.py          (ce fichier)
  - requirements.txt
  - Dockerfile
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
    # culture Internet / jargon specifique au conflit en Ukraine
    "bavovna", "banderol", "meatwave", "kinetic sanction", "fp-7", "fp-9",
]

# Mots-cles qui signalent un contexte "cible en Russie"
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
# Parametres de l'agregation par evenement
# ---------------------------------------------------------------------------

AGGREGATION_MODEL = os.environ.get("AGGREGATION_MODEL", "mistral-medium-latest")
BATCH_SIZE = 60            # posts par appel "map"
MAX_POST_CHARS = 600       # troncature du texte d'un post pour l'appel
LLM_TIMEOUT_SECONDS = 240


def _matches_filter(text: str) -> bool:
    t = text.lower()
    has_strike = any(k in t for k in STRIKE_KEYWORDS)
    has_russia = any(k in t for k in RUSSIA_CONTEXT_KEYWORDS)
    return has_strike and has_russia


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
# Activite 2 : aggregation des posts par evenement (Mistral LLM)
# ---------------------------------------------------------------------------
# Map-reduce : les posts sont groupes par lots ; chaque lot est analyse pour
# en extraire les evenements ; puis un appel final fusionne les evenements et
# synthetise chaque evenement, en signalant les contradictions entre comptes
# et en pondere les informations par la recence des posts.

MAP_PROMPT = """Tu es un analyste OSINT. Voici une liste numerotee de posts Bluesky (comptes OSINT et de presse couvrant les frappes ukrainiennes sur le territoire russe).

Tache : regroupe les posts qui rapportent ou commentent LE MEME evenement concret (meme cible, meme lieu, meme moment). Un meme evenement peut etre vu par plusieurs comptes ; un post qui corrige une information anterieure appartient au meme evenement.

Pour chaque evenement identifie, retourne :
- "title" : titre court et factuel (en francais),
- "post_ids" : les numeros des posts qui s'y rapportent,
- "hint" : une phrase resume de l'evenement.

Un post ne peut appartenir qu'a un seul evenement (celui qu'il decrit principalement). Un post purement general (pas un evenement concret) doit etre ignore.

Reponds UNIQUEMENT avec un objet JSON valide de la forme exacte :
{{"events": [{{"title": "...", "post_ids": [0], "hint": "..."}}]}}

Posts :
{posts}"""

REDUCE_PROMPT = """Tu es un analyste OSINT charge de produire la synthese finale d'un rapport quotidien sur les frappes ukrainiennes sur le territoire russe.

Tu disposes d'evenements candidats, extraits de plusieurs lots de posts, avec pour chacun les posts sources (numero, auteur, horodatage, texte).

Taches :
1. Fusionne les evenements candidats qui decrivent en realite le meme evenement (leurs post_ids doivent etre reunis).
2. Pour chaque evenement fusionne, redige (en francais) :
   - "title" : titre court et factuel,
   - "summary" : synthese agregee des informations disponibles (5 a 10 phrases maximum). Donne priorite aux posts les plus RECENTS : ils ont la plus grande probabilite d'etre exacts, surtout s'ils signalent que des informations anterieures etaient erronees. Integre les corrections et les details confirmes par plusieurs comptes.
   - "disagreements" : si des comptes se contredisent sur cet evenement, liste les desaccords en precisant QUEL compte dit QUOI (tableau de chaines, chaque entree au format "@compte : affirmation"). Sinon tableau vide.
   - "latest_correction" : si un post recent corrige des informations anterieures erronees, resume la correction en une phrase ; sinon chaine vide.
   - "post_ids" : tous les numeros de posts rattaches a cet evenement.
3. Ne garde QUE les evenements correspondant a une frappe ukrainienne sur le territoire russe ou directement lies (infrastructure russe ou territories occupes).

Reponds UNIQUEMENT avec un objet JSON valide de la forme exacte :
{{"events": [{{"title": "...", "summary": "...", "disagreements": ["@compte : affirmation"], "latest_correction": "...", "post_ids": [0]}}]}}

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
async def aggregate_events(collected: dict[str, Any]) -> dict[str, Any]:
    """Regroupe les posts par evenement et synthetise chaque evenement.

    Retourne {"events": [...], "mode": "llm" | "fallback", "error": ...}.
    En cas d'absence de cle API ou d'erreur de l'API, mode="fallback"
    et events=[].
    """
    posts: list[dict[str, Any]] = collected["posts"]
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

    try:
        last_raw = ""
        # ---- Etape "map" : extraction d'evenements par lot de posts ----
        candidate_events: list[dict[str, Any]] = []
        for start in range(0, len(posts), BATCH_SIZE):
            batch = posts[start:start + BATCH_SIZE]
            lines = "\n".join(
                _compact_post(start + i, p) for i, p in enumerate(batch)
            )
            raw = await _chat(MAP_PROMPT.format(posts=lines))
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

        raw = await _chat(REDUCE_PROMPT.format(events=events_repr))
        last_raw = raw
        data = _parse_json_content(raw)
        events: list[dict[str, Any]] = []
        for ev in _get_events(data):
            ids = sorted(set(
                int(i) for i in ev.get("post_ids", [])
                if str(i).lstrip("-").isdigit() and 0 <= int(i) < len(posts)
            ))
            if not ids:
                continue
            disagreements = [str(d) for d in ev.get("disagreements", []) if d]
            events.append({
                "title": str(ev.get("title", "") or "")[:200],
                "summary": str(ev.get("summary", "")).strip(),
                "disagreements": disagreements,
                "latest_correction": str(ev.get("latest_correction", "") or ""),
                "post_ids": ids,
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
async def compile_report(collected: dict[str, Any], aggregation: dict[str, Any],
                         since_iso: str, run_iso: str) -> dict[str, Any]:
    """Compile le rapport Markdown : evenement par evenement (classes par
    nombre de posts), ou compte par compte en cas de repli."""
    posts = collected["posts"]
    stats = collected["stats"]
    events = sorted(aggregation.get("events", []),
                    key=lambda e: len(e.get("post_ids", [])), reverse=True)
    mode = aggregation.get("mode", "fallback")
    error = aggregation.get("error")

    lines = [
        f"# Frappes ukrainiennes sur le territoire russe - rapport du {run_iso}",
        "",
        f"Fenetre : posts depuis {since_iso} (dernieres {lookback} h).",
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

    if mode == "llm" and events:
        lines.append(f"{len(events)} evenements identifies, classes du plus rapporte au moins rapporte.")
        lines.append("")
        for rank, ev in enumerate(events, start=1):
            nb = len(ev["post_ids"])
            lines.append(f"## Evenement {rank} : {ev['title']} ({nb} posts)")
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
                p = posts[pid]
                lines.append(f"- @{p['author']} - {p['posted_at']} - {p['https_url'] or p['url']}")
            lines.append("")
    else:
        if error:
            lines.append(f"_Agregation par evenement indisponible ({error}) ; format compte par compte._")
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
    return {"markdown": "\n".join(lines), "posts": posts,
            "stats": stats, "events": events, "mode": mode}


# ---------------------------------------------------------------------------
# Workflow
# ---------------------------------------------------------------------------

@workflows.workflow.define(
    name="bluesky_strike_monitor",
    workflow_display_name="Bluesky Strike Monitor",
    workflow_description="Rapport quotidien des posts Bluesky sur les frappes ukrainiennes en Russie, agrege par evenement.",
)
class BlueskyStrikeMonitor:
    @workflows.workflow.entrypoint
    async def run(self) -> str:
        lookback = 12  # les frappes ont lieu de nuit : 12 h couvrent 20h-8h
        now = workflows.workflow.now()
        since = now - timedelta(hours=lookback)
        since_iso = since.isoformat()
        run_iso = now.isoformat()

        collected = await fetch_matching_posts(since_iso)
        aggregation = await aggregate_events(collected)
        report = await compile_report(collected, aggregation, since_iso, run_iso, lookback)
        return report["markdown"]


# ---------------------------------------------------------------------------
# Point d'entree du worker
# ---------------------------------------------------------------------------

async def main() -> None:
    await workflows.run_worker([BlueskyStrikeMonitor])


if __name__ == "__main__":
    asyncio.run(main())
