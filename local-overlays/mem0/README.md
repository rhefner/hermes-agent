# Local Mem0 overlay (Richard's fork — not upstream)

Upstream moved Mem0 out of core: `plugin-catalog/mem0.yaml` pins `mem0ai/mem0@43849c6e`
(`integrations/hermes-plugin-mem0`, v1.3.0) and `hermes_cli/memory_provider_migration.py` installs it into
`$HERMES_HOME/plugins/mem0` on `hermes update` / first agent start when `memory.provider: mem0` is configured.
Two things the old bundled provider had are NOT in the vendor plugin; this directory carries them.

## 1. NLP lemmatization / BM25  (committed: `mem0-nlp` extra in pyproject.toml + uv.lock)

    hermes pm install --extra mem0-nlp        # mem0ai[nlp]==2.0.10, fastembed==0.8.1, en_core_web_sm 3.8.0
                                              # (wheel sha256 1932429db727d4bff3deed6b34cfc05df17794f4a52eeb26cf8928f7c1a0fb85)

The extra is sticky: a later `hermes plugins install mem0` rebuilds the environment with the extra still selected.
The model rides on a core extra because plugin dependency declarations cannot carry direct URLs
(`pm.plugin_declarations.applicable_requirements` drops them).

## 2. Read-only profiles  (`mem0-plugin-read-only.patch`)

The vendor plugin has no `read_only` / `MEM0_READ_ONLY` support: with the vendor plugin, profiles whose
mem0.json says `"read_only": true` (coding, media, ops, research, reviewer) would silently become READ-WRITE.
`mem0-plugin-read-only.patch` is the old port (b9ae948fae) re-targeted at the vendor plugin's `__init__.py`
(applies to pin 43849c6e with offsets only). Apply it to the installed plugin tree of every home that has one:

    patch -p1 -d "$HERMES_HOME/plugins/mem0" < local-overlays/mem0/mem0-plugin-read-only.patch

`hermes plugins install --force mem0` / a catalog pin bump overwrites the tree, so re-run the patch after either.
The durable fix is a fork of `integrations/hermes-plugin-mem0` carrying this patch, installed with
`hermes plugins install <owner>/<repo> --ref <sha>` (same plugin name `mem0`, so memory.provider is unchanged).
