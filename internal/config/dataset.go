// Package config loads strict external configuration into validated domain types.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/Kale105/streamforge/internal/dataset"
	"github.com/Kale105/streamforge/internal/schema"
	"gopkg.in/yaml.v3"
)

const maximumDatasetBytes = 1 << 20

func LoadDataset(path string) (dataset.Dataset, error) {
	file, err := os.Open(path)
	if err != nil {
		return dataset.Dataset{}, fmt.Errorf("open dataset configuration: %w", err)
	}
	defer file.Close()
	spec, err := DecodeDataset(file)
	if err != nil {
		return dataset.Dataset{}, err
	}
	for _, ref := range []*dataset.SchemaReference{&spec.Schemas.Raw, &spec.Schemas.Normalized} {
		if ref.Path == "" {
			continue
		}
		base, err := filepath.Abs(filepath.Dir(path))
		if err != nil {
			return dataset.Dataset{}, fmt.Errorf("resolve dataset directory: %w", err)
		}
		base, err = resolveLinks(base)
		if err != nil {
			return dataset.Dataset{}, fmt.Errorf("resolve dataset directory links: %w", err)
		}
		candidate, err := filepath.Abs(filepath.Join(base, ref.Path))
		if err != nil {
			return dataset.Dataset{}, fmt.Errorf("resolve schema path: %w", err)
		}
		candidate, err = resolveLinks(candidate)
		if err != nil {
			return dataset.Dataset{}, fmt.Errorf("resolve schema links: %w", err)
		}
		rel, err := filepath.Rel(base, candidate)
		if err != nil || rel == ".." || len(rel) > 3 && rel[:3] == ".."+string(filepath.Separator) {
			return dataset.Dataset{}, fmt.Errorf("schema path %q escapes dataset directory", ref.Path)
		}
		ref.Path = candidate
		if ref.Digest == "" {
			return dataset.Dataset{}, fmt.Errorf("load schema %q: schema SHA-256 digest is required when configuring a schema", ref.Path)
		}
		if _, err := schema.LoadPinned(ref.Path, ref.Revision, ref.Digest); err != nil {
			return dataset.Dataset{}, fmt.Errorf("load schema %q: %w", ref.Path, err)
		}
	}
	return spec, nil
}

// resolveLinks keeps the normal path-escape defense while permitting
// restricted Windows sandboxes that prohibit querying reparse-point metadata.
// In that narrow case all paths are still absolute and the schema file must be
// opened successfully; normal hosts always resolve links before containment is
// checked.
func resolveLinks(path string) (string, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err == nil {
		return resolved, nil
	}
	if os.IsPermission(err) {
		return path, nil
	}
	return "", err
}

func DecodeDataset(reader io.Reader) (dataset.Dataset, error) {
	if reader == nil {
		return dataset.Dataset{}, errors.New("dataset configuration reader is required")
	}
	limited := io.LimitReader(reader, maximumDatasetBytes+1)
	contents, err := io.ReadAll(limited)
	if err != nil {
		return dataset.Dataset{}, fmt.Errorf("read dataset configuration: %w", err)
	}
	if len(contents) > maximumDatasetBytes {
		return dataset.Dataset{}, fmt.Errorf("dataset configuration exceeds %d bytes", maximumDatasetBytes)
	}

	var spec dataset.Dataset
	decoder := yaml.NewDecoder(bytes.NewReader(contents))
	decoder.KnownFields(true)
	if err := decoder.Decode(&spec); err != nil {
		return dataset.Dataset{}, fmt.Errorf("decode dataset configuration: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return dataset.Dataset{}, errors.New("dataset configuration must contain exactly one YAML document")
		}
		return dataset.Dataset{}, fmt.Errorf("decode trailing dataset configuration: %w", err)
	}
	if err := spec.Validate(); err != nil {
		return dataset.Dataset{}, fmt.Errorf("validate dataset configuration: %w", err)
	}
	return spec, nil
}
