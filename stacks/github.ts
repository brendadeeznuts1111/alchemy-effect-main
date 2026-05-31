import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import * as GitHub from "alchemy/GitHub";
import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

export default Alchemy.Stack(
  "github",
  {
    providers: Layer.mergeAll(
      Cloudflare.providers(),
      GitHub.providers(),
    ),
    state: Cloudflare.state(),
  },
  Effect.gen(function* () {
    const accountId = yield* Config.string("CLOUDFLARE_ACCOUNT_ID");

    // Mint a scoped Cloudflare API token for CI — only the permissions
    // our app actually needs. Cloudflare only reveals the value once,
    // so Alchemy stores it in state automatically.
    const apiToken = yield* Cloudflare.AccountApiToken("CIToken", {
      accountId,
      policies: [
        {
          effect: "allow",
          permissionGroups: [
            "Workers Scripts Write",
            "Workers R2 Storage Write",
            "Workers Tail Read",
            "Account Settings Read",
          ],
          resources: {
            [`com.cloudflare.api.account.${accountId}`]: "*",
          },
        },
      ],
    });

    // Push the token into GitHub Actions secrets so CI can read it.
    yield* GitHub.Secret("cf-api-token", {
      owner: "brendadeeznuts1111",
      repository: "alchemy-effect-main",
      name: "CLOUDFLARE_API_TOKEN",
      value: apiToken.value,
    });

    // Also store the account ID as a GitHub variable (not a secret).
    yield* GitHub.Variable("cf-account-id", {
      owner: "brendadeeznuts1111",
      repository: "alchemy-effect-main",
      name: "CLOUDFLARE_ACCOUNT_ID",
      value: accountId,
    });

    return { done: true };
  }),
);
