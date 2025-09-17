# goals-live-bot-livescore

Bot completo usando **Livescore.in** como fonte de dados ao vivo: coleta, aprendizado online e previsão de janela de gol (HT/FT), com tracker GREEN/RED persistente.

## Rodando
```bash
npm i
npx playwright install
npm start
```

Variáveis:
- `LIVE_URL` (opcional): por ex. `https://www.livescore.in/` (default).
- `HEADLESS=0` para abrir com UI (default).

## Observações
- Livescore.in pode mudar seletores. O scraper tenta ser robusto:
  - Lista: `div.event__match`, `div.event__time`, `div.event__score--home/away`, `div.event__participant--home/away`.
  - Link da partida: `a[href*="/match/"]` para abrir e coletar stats.
  - Stats: linhas `.stat__row` com `.stat__categoryName`, `.stat__homeValue`, `.stat__awayValue` (mapeando *On target*, *Corners*, *Dangerous Attacks* / português).
- Odds não vêm do Livescore.in; a política aceita **sinal sem odds** com limiares de probabilidade (config em `src/policy/decision.js`). Se quiser usar edge vs odds, integre uma fonte de odds e preencha `g.odds`.

## Persistência
- Modelos: `./data/models/model_10.json` e `model_20.json`.
- Tracker: `./data/goals_tracker_state.json`, CSV em `./logs/goals_signals.csv`.
- **Ctrl+C** salva e retoma depois.
