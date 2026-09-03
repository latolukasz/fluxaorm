package test_generate

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These read the committed output of the generator rather than re-running it:
// TestGenerate clears the entities directory, so a test that regenerated would
// race the only copy of the code it is asserting on.

func generated(t *testing.T, name string) string {
	t.Helper()

	body, err := os.ReadFile(filepath.Join("entities", name))
	require.NoError(t, err)

	return string(body)
}

// The dispatch function is the application's whole entry point, so its shape -
// typed task pointer in, run id out - is the contract worth pinning.
func TestGeneratedDispatchNamesTheTaskStruct(t *testing.T) {
	body := generated(t, "consumers.go")

	assert.Contains(t, body, "func DispatchSendWelcomeEmail(")
	assert.Contains(t, body, "task *jobtasks.SendWelcomeEmail,")
	assert.Contains(t, body, "return fluxaorm.DispatchTask(orm, task, opts...)")
	assert.Contains(t, body, `the "emails" queue`)
}

// Two fixture task packages share the base name `jobtasks`, so the generator
// has to alias one of them. Without this the generated file would not compile.
func TestGeneratedImportsAliasCollidingTaskPackages(t *testing.T) {
	body := generated(t, "consumers.go")

	assert.Contains(t, body, `"github.com/latolukasz/fluxaorm/v2/test_fixtures/jobtasks"`)
	assert.Contains(t, body, `jobtasks2 "github.com/latolukasz/fluxaorm/v2/test_fixtures/media/jobtasks"`)
	assert.Contains(t, body, "task *jobtasks2.TranscodeClip,")
}

// A task consumer's builder exposes exactly the tasks on the queues it drains,
// so handling something it never receives is a compile error.
func TestGeneratedTaskBuilderExposesOnlyItsQueuesTasks(t *testing.T) {
	emails := generated(t, "emails-worker_consumer.go")

	assert.Contains(t, emails, "func (b *EmailsWorkerBuilder) OnSendWelcomeEmail(")
	assert.Contains(t, emails, "func (b *EmailsWorkerBuilder) OnSendReceipt(")
	assert.Contains(t, emails, "fluxaorm.BuildTaskDispatch(handler)")
	assert.NotContains(t, emails, "OnTranscodeClip",
		"TranscodeClip is on another queue, so this builder must not expose it")
	assert.NotContains(t, emails, "OnSendPasswordReset",
		"SendPasswordReset fell through to the default queue")

	media := generated(t, "media-worker_consumer.go")
	assert.Contains(t, media, "func (b *MediaWorkerBuilder) OnTranscodeClip(")
	assert.NotContains(t, media, "OnSendWelcomeEmail")
}

// An entity consumer's builder likewise exposes only its declared entities,
// and offers the batch form the task side has no use for.
func TestGeneratedEntityBuilderExposesOnlyDeclaredEntities(t *testing.T) {
	notifier := generated(t, "test-notifier_consumer.go")

	assert.Contains(t, notifier, "func (b *TestNotifierBuilder) OnGenerateEntityDirty(")
	assert.Contains(t, notifier, "func (b *TestNotifierBuilder) OnGenerateEntityDirtyBatch(")
	assert.NotContains(t, notifier, "OnGenerateEntityDirtyB(",
		"test-notifier declares only generateEntityDirty")
}

// The task builders carry no batch methods: a batch ack is all-or-nothing, and
// a task's whole point is an independent retry ladder per message.
func TestGeneratedTaskBuilderHasNoBatchMethods(t *testing.T) {
	assert.NotContains(t, generated(t, "emails-worker_consumer.go"), "Batch(")
}

// The publisher registers under the table name, not a subject: the subject
// format is fluxaorm's to own, so generated code must never spell one out.
func TestGeneratedPublisherRegistersTheTableName(t *testing.T) {
	body := generated(t, "generateEntityDirty.go")

	assert.Contains(t, body,
		`fluxaorm.RegisterEntityPublisher[GenerateEntityDirty]("generateEntityDirty", buildGenerateEntityDirtyDirtyEvent)`)
	assert.NotContains(t, body, "fluxa.entity.")
}
