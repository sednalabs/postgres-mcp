/**
 * @name Forbidden public workflow trigger
 * @description Detects workflow triggers that are not allowed in this public repository.
 * @kind problem
 * @problem.severity error
 * @security-severity 8.2
 * @precision high
 * @id actions/forbidden-public-workflow-trigger
 * @tags actions
 *       security
 *       external/cwe/cwe-266
 */

import actions
import WorkflowSecurity

from Job job, Event event
where
  event = job.getATriggerEvent() and
  forbiddenPublicTrigger(event)
select job,
  "The workflow uses the forbidden public-repository trigger '" + event.getName() +
    "'. Use pull_request with read-only permissions, or split trusted write operations into a protected workflow."
