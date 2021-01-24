package snowstore

import "golang.org/x/xerrors"

var ErrEntryNotFound = xerrors.New("entry not found")
var ErrRootParentSpecified = xerrors.New("root parent can't be specified")
