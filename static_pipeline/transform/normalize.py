import re
import unicodedata

def normalize_film_title(title: str) -> str:
    """
    Normalisiert einen Filmtitel für bessere Vergleichbarkeit.
    - Unicode-Normalisierung (Akzente etc.)
    - Kleinschreibung
    - Entfernt führende Artikel
    - Entfernt alle nicht-alphanumerischen Zeichen (außer Leerzeichen)
    - Reduziert mehrfache Leerzeichen und trimmt
    """
    if not isinstance(title, str):
        return "" # Oder einen anderen Standardwert für Nicht-Strings

    normalized_title = title

    # 1. Unicode-Normalisierung (z.B. für Akzente wie 'é' zu 'e')
    try:
        # Versuche, Akzente zu entfernen, indem in ASCII zerlegt wird
        normalized_title = str(unicodedata.normalize('NFKD', normalized_title).encode('ascii', 'ignore').decode('utf-8', 'ignore'))
    except Exception:
        # Fallback, falls die Normalisierung fehlschlägt, den Titel so belassen
        pass

    # 2. Kleinschreibung
    normalized_title = normalized_title.lower()

    # 3. Artikel am Anfang entfernen (Beispielliste, erweiterbar)
    # Wichtig: Leerzeichen nach dem Artikel, um Wörter wie "Lesley" nicht fälschlicherweise zu kürzen.
    # L' muss speziell behandelt werden.
    articles_to_remove = [
        "the ", "a ", "an ", 
        "der ", "die ", "das ", 
        "le ", "la ", "les ", 
        "un ", "une ",
        "el ", "los ", "las ",
        "il ", "lo ", "gli ", "gl "
    ]
    # Spezielle Behandlung für l' etc. am Anfang
    if normalized_title.startswith("l'") or normalized_title.startswith("d'"): # erweiterbar
        normalized_title = normalized_title[2:] # Entferne die ersten beiden Zeichen

    for article in articles_to_remove:
        if normalized_title.startswith(article):
            normalized_title = normalized_title[len(article):]
            # break # Nur den ersten gefundenen Artikel entfernen, oder alle? Für Merge ist erster meist besser.
    
    # 4. Alle nicht-alphanumerischen Zeichen (außer Leerzeichen) entfernen
    # \w ist [a-zA-Z0-9_]. Wir behalten Leerzeichen explizit.
    normalized_title = re.sub(r'[^\w\s]', '', normalized_title)

    # 5. Mehrfache Leerzeichen zu einem einzigen reduzieren und an den Enden entfernen
    normalized_title = re.sub(r'\s+', ' ', normalized_title).strip()
    
    return normalized_title 