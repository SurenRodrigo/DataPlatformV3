import {execSync} from 'child_process';

import {HasuraClient} from './hasura-client';

let hasuraAdminSecret: string;
let hasuraClient: HasuraClient;

beforeAll(async () => {
  hasuraAdminSecret = process.env.HASURA_GRAPHQL_ADMIN_SECRET;

  hasuraClient = new HasuraClient('http://localhost:8080', hasuraAdminSecret);
  await hasuraClient.waitUntilHealthy();
}, 60 * 1000);

describe('integration tests', () => {

  test(
    'check CI event writes in Hasura',
    async () => {
      const origin = 'CI_test';

      sendCIEvent(origin);

      expect(await hasuraClient.getCicdBuildCount(origin)).toBe(1);
      expect(await hasuraClient.getCicdPipelineCount(origin)).toBe(1);
      expect(await hasuraClient.getCicdOrganizationCount(origin)).toBe(1);
      expect(await hasuraClient.getCicdArtifactCount(origin)).toBe(1);
      expect(
        await hasuraClient.getCicdArtifactCommitAssociationCount(origin)
      ).toBe(1);
      expect(await hasuraClient.getCicdRepositoryCount(origin)).toBe(1);
      expect(await hasuraClient.getVcsPullRequestCommitCount(origin)).toBe(1);
    },
    60 * 1000
  );

  test(
    'check CD event writes in Hasura',
    async () => {
      const origin = 'CD_test';

      sendCDEvent(origin);

      expect(await hasuraClient.getComputeApplicationCount(origin)).toBe(1);
      expect(await hasuraClient.getCicdArtifactDeploymentCount(origin)).toBe(1);
      expect(await hasuraClient.getCicdArtifactCount(origin)).toBe(1);
      expect(await hasuraClient.getCicdDeploymentCount(origin)).toBe(1);
      expect(await hasuraClient.getCicdBuildCount(origin)).toBe(1);
      expect(await hasuraClient.getCicdPipelineCount(origin)).toBe(1);
      expect(await hasuraClient.getCicdOrganizationCount(origin)).toBe(1);
    },
    60 * 1000
  );



  function sendCIEvent(origin: string) {
    execSync(`docker pull farosai/faros-events-cli:latest \
    && docker run -i --network host farosai/faros-events-cli:latest CI \
    --run "GitHub://faros-ai/faros-community-edition/123" \
    --commit "GitHub://faros-ai/faros-community-edition/XYZ" \
    --pull_request_number 1 \
    --artifact "GitHub://faros-ai/faros-community-edition/456" \
    --run_status "Success" \
    --run_status_details "Some extra details" \
    --run_start_time "1000" \
    --run_end_time "2000" \
    --hasura_admin_secret ${hasuraAdminSecret} \
    --origin ${origin} \
    --community_edition`);
  }

  function sendCDEvent(origin: string) {
    execSync(`docker pull farosai/faros-events-cli:latest \
    && docker run -i --network host farosai/faros-events-cli:latest CD \
    --run "GitHub://soraf-ai/faros-community-edition/123" \
    --artifact "GitHub://soraf-ai/faros-community-edition/456" \
    --run_status "Success" \
    --run_status_details "Some extra details" \
    --run_start_time "1000" \
    --run_end_time "2000" \
    --deploy "GitHub://faros/prod/789" \
    --deploy_status "Success" \
    --deploy_start_time "3000" \
    --deploy_end_time "4000" \
    --hasura_admin_secret ${hasuraAdminSecret} \
    --origin ${origin} \
    --community_edition`);
  }
});
