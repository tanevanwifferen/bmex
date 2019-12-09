[![Codacy Badge](https://api.codacy.com/project/badge/Grade/21f103c475e44fa4b30936f06bb5088f)](https://www.codacy.com/manual/dxflores/bmex?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=dxflores/bmex&amp;utm_campaign=Badge_Grade)
# bmex

Lets you download historical data, both [quotes](https://www.bitmex.com/api/explorer/#!/Quote/Quote_get) (top bid/ask) and [trades](https://www.bitmex.com/api/explorer/#!/Trade/Trade_get), for all symbols and indices (including delisted ones) from BitMEX + stores it by creating the directory structure shown below.

Example: `python bmex.py --symbols XBTUSD ETHUSD --channels quotes trades --start 2018-10-30 --end 2019-11-01`

```
current_directory # where you run the code
  └──BITMEX/
      ├── ETHUSD
      │   ├── quotes
      │   │   └── 2018
      │   │       ├── 10
      │   │       │   ├── 2018-10-30.csv
      │   │       │   └── 2018-10-31.csv
      │   │       └── 11
      │   │           └── 2018-11-01.csv
      │   └── trades
      │       └── 2018
      │           ├── 10
      │           │   ├── 2018-10-30.csv
      │           │   └── 2018-10-31.csv
      │           └── 11
      │               └── 2018-11-01.csv
      └── XBTUSD
          ├── quotes
          │   └── 2018
          │       ├── 10
          │       │   ├── 2018-10-30.csv
          │       │   └── 2018-10-31.csv
          │       └── 11
          │           └── 2018-11-01.csv
          └── trades
              └── 2018
                  ├── 10
                  │   ├── 2018-10-30.csv
                  │   └── 2018-10-31.csv
                  └── 11
                      └── 2018-11-01.csv
```
## Notes
- In addition to the parameters shown in the example above, you can pass an extra parameter "--save_to" to create the directory structure at a preferred path.
- Confirm that you have the necessary storage space. To give you an idea, as of December 2019, a full backfill of only XBTUSD will require ~125G of free space (~75G quotes + ~50G trades).
