package java

import "testing"

func TestPromptMetadata(t *testing.T) {
	implementation := New()
	if implementation.Name() != "Java" || implementation.InitializeCommand() != "mvn -B archetype:generate -DgroupId=tokenbench -DartifactId=token-bench-target -DarchetypeGroupId=org.apache.maven.archetypes -DarchetypeArtifactId=maven-archetype-quickstart -DarchetypeVersion=1.5 -DjavaCompilerVersion=21 -DinteractiveMode=false && cp -R token-bench-target/. . && rm -rf token-bench-target" {
		t.Fatalf("unexpected prompt metadata: %q %q", implementation.Name(), implementation.InitializeCommand())
	}
	if implementation.TaskPromptSuffix() != "" {
		t.Fatalf("unexpected task prompt suffix: %q", implementation.TaskPromptSuffix())
	}
}

func TestStartProjectRejectsInvalidPort(t *testing.T) {
	if _, err := New().StartProject(t.TempDir(), 0); err == nil {
		t.Fatal("expected invalid port error")
	}
}
