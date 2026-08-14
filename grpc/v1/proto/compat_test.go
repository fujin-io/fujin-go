package proto

import "testing"

func TestClientDescriptorsAvoidCanonicalServerNamespace(t *testing.T) {
	if got := string(File_fujin_proto.Package()); got == "fujin.v1" {
		t.Fatalf("client descriptors use canonical server namespace %q and will panic when both packages are imported", got)
	}
	if got, want := FujinService_Stream_FullMethodName, "/fujin.v1.FujinService/Stream"; got != want {
		t.Fatalf("gRPC method path = %q, want %q", got, want)
	}
}
