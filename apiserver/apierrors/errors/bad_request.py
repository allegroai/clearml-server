from apiserver.apierrors.base import BaseError


class NotSupported(BaseError):
    _default_code = 400
    _default_subcode = 1
    _default_msg = "Endpoint is not supported"


class RequestPathHasInvalidVersion(BaseError):
    _default_code = 400
    _default_subcode = 2
    _default_msg = "Request path has invalid version"


class InvalidHeaders(BaseError):
    _default_code = 400
    _default_subcode = 5
    _default_msg = "Invalid headers"


class ImpersonationError(BaseError):
    _default_code = 400
    _default_subcode = 6
    _default_msg = "Impersonation error"


class InvalidId(BaseError):
    _default_code = 400
    _default_subcode = 10
    _default_msg = "Invalid object id"


class MissingRequiredFields(BaseError):
    _default_code = 400
    _default_subcode = 11
    _default_msg = "Missing required fields"


class ValidationError(BaseError):
    _default_code = 400
    _default_subcode = 12
    _default_msg = "Validation error"


class FieldsNotAllowedForRole(BaseError):
    _default_code = 400
    _default_subcode = 13
    _default_msg = "Fields not allowed for role"


class InvalidFields(BaseError):
    _default_code = 400
    _default_subcode = 14
    _default_msg = "Fields not defined for object"


class FieldsConflict(BaseError):
    _default_code = 400
    _default_subcode = 15
    _default_msg = "Conflicting fields"


class FieldsValueError(BaseError):
    _default_code = 400
    _default_subcode = 16
    _default_msg = "Invalid value for fields"


class BatchContainsNoItems(BaseError):
    _default_code = 400
    _default_subcode = 17
    _default_msg = "Batch request contains no items"


class BatchValidationError(BaseError):
    _default_code = 400
    _default_subcode = 18
    _default_msg = "Batch request validation error"


class InvalidLuceneSyntax(BaseError):
    _default_code = 400
    _default_subcode = 19
    _default_msg = "Malformed lucene query"


class FieldsTypeError(BaseError):
    _default_code = 400
    _default_subcode = 20
    _default_msg = "Invalid type for fields"


class InvalidRegexError(BaseError):
    _default_code = 400
    _default_subcode = 21
    _default_msg = "Malformed regular expression"


class InvalidEmailAddress(BaseError):
    _default_code = 400
    _default_subcode = 22
    _default_msg = "Malformed email address"


class InvalidDomainName(BaseError):
    _default_code = 400
    _default_subcode = 23
    _default_msg = "Malformed domain name"


class NotPublicObject(BaseError):
    _default_code = 400
    _default_subcode = 24
    _default_msg = "Object is not public"


class InvalidAccessKey(BaseError):
    _default_code = 400
    _default_subcode = 75
    _default_msg = "Access key not found"


class TaskError(BaseError):
    _default_code = 400
    _default_subcode = 100
    _default_msg = "General task error"


class InvalidTaskId(BaseError):
    _default_code = 400
    _default_subcode = 101
    _default_msg = "Invalid task id"


class TaskValidationError(BaseError):
    _default_code = 400
    _default_subcode = 102
    _default_msg = "Task validation error"


class InvalidTaskStatus(BaseError):
    _default_code = 400
    _default_subcode = 110
    _default_msg = "Invalid task status"


class TaskNotStarted(BaseError):
    _default_code = 400
    _default_subcode = 111
    _default_msg = "Task not started (invalid task status)"


class TaskInProgress(BaseError):
    _default_code = 400
    _default_subcode = 112
    _default_msg = "Task in progress (invalid task status)"


class TaskPublished(BaseError):
    _default_code = 400
    _default_subcode = 113
    _default_msg = "Task published (invalid task status)"


class TaskStatusUnknown(BaseError):
    _default_code = 400
    _default_subcode = 114
    _default_msg = "Task unknown (invalid task status)"


class InvalidTaskExecutionProgress(BaseError):
    _default_code = 400
    _default_subcode = 120
    _default_msg = "Invalid task execution progress"


class FailedChangingTaskStatus(BaseError):
    _default_code = 400
    _default_subcode = 121
    _default_msg = "Failed changing task status. probably someone changed it before you"


class MissingTaskFields(BaseError):
    _default_code = 400
    _default_subcode = 122
    _default_msg = "Task is missing expected fields"


class TaskCannotBeDeleted(BaseError):
    _default_code = 400
    _default_subcode = 123
    _default_msg = "Task cannot be deleted"


class TaskHasJobsRunning(BaseError):
    _default_code = 400
    _default_subcode = 125
    _default_msg = "Task has jobs that haven't completed yet"


class InvalidTaskType(BaseError):
    _default_code = 400
    _default_subcode = 126
    _default_msg = "Invalid task type for this operations"


class InvalidTaskInput(BaseError):
    _default_code = 400
    _default_subcode = 127
    _default_msg = "Invalid task output"


class InvalidTaskOutput(BaseError):
    _default_code = 400
    _default_subcode = 128
    _default_msg = "Invalid task output"


class TaskPublishInProgress(BaseError):
    _default_code = 400
    _default_subcode = 129
    _default_msg = "Task publish in progress"


class TaskNotFound(BaseError):
    _default_code = 400
    _default_subcode = 130
    _default_msg = "Task not found"


class EventsNotAdded(BaseError):
    _default_code = 400
    _default_subcode = 131
    _default_msg = "Events not added"


class OperationSupportedOnReportsOnly(BaseError):
    _default_code = 400
    _default_subcode = 150
    _default_msg = "Passed task is not report"


class CannotRemoveAllRuns(BaseError):
    _default_code = 400
    _default_subcode = 160
    _default_msg = "At least one pipeline run should be left"


class ModelError(BaseError):
    _default_code = 400
    _default_subcode = 200
    _default_msg = "General task error"


class InvalidModelId(BaseError):
    _default_code = 400
    _default_subcode = 201
    _default_msg = "Invalid model id"


class ModelNotReady(BaseError):
    _default_code = 400
    _default_subcode = 202
    _default_msg = "Model is not ready"


class ModelIsReady(BaseError):
    _default_code = 400
    _default_subcode = 203
    _default_msg = "Model is ready"


class InvalidModelUri(BaseError):
    _default_code = 400
    _default_subcode = 204
    _default_msg = "Invalid model uri"


class ModelInUse(BaseError):
    _default_code = 400
    _default_subcode = 205
    _default_msg = "Model is used by tasks"


class ModelCreatingTaskExists(BaseError):
    _default_code = 400
    _default_subcode = 206
    _default_msg = "Task that created this model exists"


class InvalidUser(BaseError):
    _default_code = 400
    _default_subcode = 300
    _default_msg = "Invalid user"


class InvalidUserId(BaseError):
    _default_code = 400
    _default_subcode = 301
    _default_msg = "Invalid user id"


class UserIdExists(BaseError):
    _default_code = 400
    _default_subcode = 302
    _default_msg = "User id already exists"


class InvalidPreferencesUpdate(BaseError):
    _default_code = 400
    _default_subcode = 305
    _default_msg = "Malformed key and/or value"


class InvalidProjectId(BaseError):
    _default_code = 400
    _default_subcode = 401
    _default_msg = "Invalid project id"


class ProjectHasTasks(BaseError):
    _default_code = 400
    _default_subcode = 402
    _default_msg = "Project has associated tasks"


class ProjectNotFound(BaseError):
    _default_code = 400
    _default_subcode = 403
    _default_msg = "Project not found"


class ProjectHasModels(BaseError):
    _default_code = 400
    _default_subcode = 405
    _default_msg = "Project has associated models"


class ProjectHasDatasets(BaseError):
    _default_code = 400
    _default_subcode = 406
    _default_msg = "Project has associated non-empty datasets"


class InvalidProjectName(BaseError):
    _default_code = 400
    _default_subcode = 407
    _default_msg = "Invalid project name"


class CannotUpdateProjectLocation(BaseError):
    _default_code = 400
    _default_subcode = 408
    _default_msg = "Cannot update project location. use projects.move instead"


class ProjectPathExceedsMax(BaseError):
    _default_code = 400
    _default_subcode = 409
    _default_msg = "Project path exceed the maximum allowed depth"


class ProjectSourceAndDestinationAreTheSame(BaseError):
    _default_code = 400
    _default_subcode = 410
    _default_msg = "Project has the same source and destination paths"


class ProjectCannotBeMovedUnderItself(BaseError):
    _default_code = 400
    _default_subcode = 411
    _default_msg = "Project can not be moved under itself in the projects hierarchy"


class ProjectCannotBeMergedIntoItsChild(BaseError):
    _default_code = 400
    _default_subcode = 412
    _default_msg = "Project can not be merged into its own child"


class ProjectHasPipelines(BaseError):
    _default_code = 400
    _default_subcode = 413
    _default_msg = "Project has associated pipelines with active controllers"


class InvalidQueueId(BaseError):
    _default_code = 400
    _default_subcode = 701
    _default_msg = "Invalid queue id"


class QueueNotEmpty(BaseError):
    _default_code = 400
    _default_subcode = 702
    _default_msg = "Queue is not empty"


class InvalidQueueOrTaskNotQueued(BaseError):
    _default_code = 400
    _default_subcode = 703
    _default_msg = "Invalid queue id or task not in queue"


class RemovedDuringReposition(BaseError):
    _default_code = 400
    _default_subcode = 704
    _default_msg = "Task was removed by another party during reposition"


class FailedAddingDuringReposition(BaseError):
    _default_code = 400
    _default_subcode = 705
    _default_msg = "Failed adding task back to queue during reposition"


class TaskAlreadyQueued(BaseError):
    _default_code = 400
    _default_subcode = 706
    _default_msg = "Failed adding task to queue since task is already queued"


class NoDefaultQueue(BaseError):
    _default_code = 400
    _default_subcode = 707
    _default_msg = "No queue is tagged as the default queue for this company"


class MultipleDefaultQueues(BaseError):
    _default_code = 400
    _default_subcode = 708
    _default_msg = "More than one queue is tagged as the default queue for this company"


class DataValidationError(BaseError):
    _default_code = 400
    _default_subcode = 800
    _default_msg = "Data validation error"


class ExpectedUniqueData(BaseError):
    _default_code = 400
    _default_subcode = 801
    _default_msg = "Value combination already exists (unique field already contains this value)"


class InvalidWorkerId(BaseError):
    _default_code = 400
    _default_subcode = 1001
    _default_msg = "Invalid worker id"


class WorkerRegistrationFailed(BaseError):
    _default_code = 400
    _default_subcode = 1002
    _default_msg = "Worker registration failed"


class WorkerRegistered(BaseError):
    _default_code = 400
    _default_subcode = 1003
    _default_msg = "Worker is already registered"


class WorkerNotRegistered(BaseError):
    _default_code = 400
    _default_subcode = 1004
    _default_msg = "Worker is not registered"


class WorkerStatsNotFound(BaseError):
    _default_code = 400
    _default_subcode = 1005
    _default_msg = "Worker stats not found"


class InvalidScrollId(BaseError):
    _default_code = 400
    _default_subcode = 1104
    _default_msg = "Invalid scroll id"

class InvalidPipelineId(BaseError):
    _default_code = 400
    _default_subcode = 401
    _default_msg = "Invalid project id"

class InvalidStepId(BaseError):
    _default_code = 400
    _default_subcode = 401
    _default_msg = "Invalid project id"
