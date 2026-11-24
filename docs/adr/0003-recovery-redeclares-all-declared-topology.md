# Recovery re-declares all declared topology

When a channel recovers, it re-declares every topology element marked `Declare: true` —
durable and ephemeral alike. AMQP declarations are idempotent when the same parameters
are re-fed, so re-declaring is safe and effectively free, and a recovered client finds
all the infrastructure it expects in place, ready to transact seamlessly, regardless of
what the broker retained: durable definitions survive an ordinary broker restart, but a
reset or replaced node loses even durable ones — re-declaration covers that hole too.

The prior implementation skipped durable elements during recovery deliberately, as an
optimization: durable infrastructure was taken for granted to still be there, so the
re-declare call was considered redundant. That hidden assumption — the broker always
retains durable definitions — fails on a reset or replaced node, and since the call is
idempotent at the server anyway, the saving is not worth the gap it leaves. Hence this
decision to always re-declare.

The trade-off: if a durable element's live definition has drifted from its configured
parameters, the re-declare fails with AMQP 406 (precondition failure) instead of
silently continuing with the broker's divergent version. That is deliberate — drift
should surface loudly. In grabbit the blast radius is contained: topology runs on an
isolated channel, and per-element failures already surface as error events rather than
aborting the channel's recovery.

**Status**: accepted; implemented. `makeTopology` re-declares every element marked
`Declare: true` on recovery, durable included; the former skip-durable branch and its
dead `recovering` parameter are gone.
