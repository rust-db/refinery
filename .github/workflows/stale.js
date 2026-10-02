/**
 * Stale issues check.
 *
 * An issue becomes stale only when the maintainer gave the last reply and the
 * author has not answered since. Issues where the ball is with the maintainer,
 * with the community, or with nobody, are never marked. If a stale-labelled
 * issue receives a reply from anyone other than the maintainer, the label is
 * removed again.
 *
 * Invoked by .github/workflows/stale.yml through actions/github-script, which
 * injects the `github` (octokit) client and the workflow `context`.
 */

const MAINTAINER_ASSOCIATIONS = ['OWNER', 'MEMBER', 'COLLABORATOR'];
// Days after a maintainer's last reply before an issue is marked stale
const STALE_DAYS = 30;
// Days after marking stale before the issue is closed
const CLOSE_DAYS = 7;
// Label used to mark stale issues (same label the old bot used)
const STALE_LABEL = 'Stale';
// Manual escape hatch: issues with this label are never touched
const EXEMPT_LABEL = 'keep-open';
// Bot accounts whose comments do not count as a turn in the conversation
const IGNORED_AUTHORS = ['github-actions[bot]'];

module.exports = main;

/**
 * Walks all open issues and applies the stale rules.
 */
async function main({ github, context }) {
  const log = (...args) => console.log(...args);

  // Runs an API operation and turns a failure into a log line, so one locked
  // or already-closed issue cannot stop the run.
  const attempt = async (description, operation) => {
    try {
      await operation();
    } catch (error) {
      log(`ERROR while ${description}: ${error.message}`);
    }
  };

  const issues = await listOpen(github, context);
  log(`checked ${issues.length} open issues`);

  for (const issue of issues) {
    const labels = issue.labels.nodes.map((label) => label.name);
    if (labels.includes(EXEMPT_LABEL)) {
      log(`#${issue.number}: exempt (${EXEMPT_LABEL})`);
      continue;
    }
    if (MAINTAINER_ASSOCIATIONS.includes(issue.authorAssociation)) {
      log(`#${issue.number}: opened by the maintainers, skipped`);
      continue;
    }

    const last = lastHumanTurn(issue);
    const lastSpeaker = last.author ? last.author.login : 'nobody';
    const ageDays = daysSince(last.createdAt);
    const hasStale = labels.includes(STALE_LABEL);

    // If the ball is with a maintainer, with the community, or nobody replied,
    // the issue should never be stale.
    if (!MAINTAINER_ASSOCIATIONS.includes(last.association)) {
      if (hasStale) {
        await attempt(`removing ${STALE_LABEL} from #${issue.number} (last reply from ${lastSpeaker})`, () =>
          github.rest.issues.removeLabel({
            owner: context.repo.owner,
            repo: context.repo.repo,
            issue_number: issue.number,
            name: STALE_LABEL,
          })
        );
      }
      continue;
    }

    // A maintainer gave the last reply: the author owes an answer.
    if (!hasStale && ageDays >= STALE_DAYS) {
      await attempt(`marking #${issue.number} stale (author silent for ${Math.floor(ageDays)}d)`, async () => {
        await github.rest.issues.addLabels({
          owner: context.repo.owner,
          repo: context.repo.repo,
          issue_number: issue.number,
          labels: [STALE_LABEL],
        });
        await github.rest.issues.createComment({
          owner: context.repo.owner,
          repo: context.repo.repo,
          issue_number: issue.number,
          body: `This issue is waiting for a reply from the author. It will be closed in ${CLOSE_DAYS} days if there is no further activity.`,
        });
      });
    } else if (hasStale && ageDays >= STALE_DAYS + CLOSE_DAYS) {
      await attempt(`closing #${issue.number} (no author reply for ${Math.floor(ageDays)}d)`, async () => {
        await github.rest.issues.createComment({
          owner: context.repo.owner,
          repo: context.repo.repo,
          issue_number: issue.number,
          body: 'Closing this issue: no reply from the author.',
        });
        await github.rest.issues.update({
          owner: context.repo.owner,
          repo: context.repo.repo,
          issue_number: issue.number,
          state: 'closed',
          state_reason: 'not_planned',
        });
      });
    }
  }
}

/** All open issues and pull requests, fetched with the same query template. */
async function listOpen(github, context) {
  // The query template, built once. GraphQL does not accept field names as
  // variables, so the connection ('issues' or 'pullRequests') is substituted in.
  const OPEN_QUERY = (connection) => `
    query ($owner: String!, $name: String!, $cursor: String) {
      repository(owner: $owner, name: $name) {
        ${connection}(first: 50, after: $cursor, states: OPEN) {
          pageInfo { hasNextPage endCursor }
          nodes {
            number
            createdAt
            authorAssociation
            author { login }
            labels(first: 30) { nodes { name } }
            comments(last: 10) { nodes { createdAt authorAssociation author { login } } }
          }
        }
      }
    }`;

  const fetch = async (connection) => {
    const nodes = [];
    let cursor = null;
    do {
      const data = await github.graphql(OPEN_QUERY(connection), {
        owner: context.repo.owner,
        name: context.repo.repo,
        cursor,
      });
      const page = data.repository[connection];
      nodes.push(...page.nodes);
      cursor = page.pageInfo.hasNextPage ? page.pageInfo.endCursor : null;
    } while (cursor);
    return nodes;
  };

  const [issues, pulls] = await Promise.all([fetch('issues'), fetch('pullRequests')]);
  return [...issues, ...pulls];
}

/**
 * The most recent comment from a human, ignoring our own bot (stale markers
 * and closing notes). Falls back to the issue body when nobody has replied.
 */
function lastHumanTurn(issue) {
  const comments = issue.comments.nodes.filter(
    (comment) => comment.author && !IGNORED_AUTHORS.includes(comment.author.login.toLowerCase())
  );
  if (comments.length > 0) {
    const comment = comments[comments.length - 1];
    return { author: comment.author, createdAt: comment.createdAt, association: comment.authorAssociation };
  }
  return { author: issue.author, createdAt: issue.createdAt, association: issue.authorAssociation };
}

function daysSince(isoDate) {
  return (Date.now() - Date.parse(isoDate)) / (24 * 60 * 60 * 1000);
}
