import * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";

const ACCOUNT_ID = "7a470541a704caaf91e71efccc78fd36";

export default Alchemy.Stack(
  "github",
  {
    providers: Cloudflare.providers(),
    state: Cloudflare.state(),
  },
  Effect.gen(function* () {
    const apiToken = yield* Cloudflare.AccountApiToken("CIToken", {
      accountId: ACCOUNT_ID,
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
            [`com.cloudflare.api.account.${ACCOUNT_ID}`]: "*",
          },
        },
      ],
    });

    return {
      accountId: ACCOUNT_ID,
      tokenId: apiToken.tokenId,
    };
  }),
);
