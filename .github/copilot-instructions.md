# RustFS Development Instructions

Always reference these instructions first and fallback to search or bash commands only when you encounter unexpected information that does not match the info here.

## Working Effectively

### Bootstrap and Build
- Install cargo-nextest for faster testing: `cargo install cargo-nextest`
- Basic check (5-6 minutes): `cargo check --all-targets`
- **NEVER CANCEL**: Release build takes 15+ minutes. Set timeout to 30+ minutes: `cargo build --release -p rustfs --bins`
- **NEVER CANCEL**: Development build takes 8-12 minutes: `cargo build -p rustfs --bins`
- Console assets build: `touch rustfs/build.rs` before building to embed console resources
- Platform-specific build: `./build-rustfs.sh --platform x86_64-unknown-linux-musl` (10+ minutes, **NEVER CANCEL**)

### Testing
- **NEVER CANCEL**: Unit tests take 8-15 minutes: `cargo nextest run --all --exclude e2e_test`
- **NEVER CANCEL**: Doc tests take 2-3 minutes: `cargo test --all --doc`
- **NEVER CANCEL**: Full test suite: `make test` (15+ minutes total)
- E2E tests require s3s-e2e tool: `cargo install s3s-e2e --git https://github.com/Nugine/s3s.git --rev b7714bfaa17ddfa9b23ea01774a1e7bbdbfc2ca3`
- E2E test run: `./scripts/e2e-run.sh ./target/debug/rustfs /tmp/rustfs`

### Code Quality (Pre-commit Requirements)
- Format code (3 seconds): `cargo fmt --all`
- Check formatting: `cargo fmt --all --check`
- **NEVER CANCEL**: Clippy lints take 10+ minutes: `cargo clippy --all-targets --all-features -- -D warnings`
- Compilation check (5+ minutes): `cargo check --all-targets`
- All pre-commit checks: `make pre-commit` (**NEVER CANCEL**: 20+ minutes total)

### Quick Commands with Make
```bash
make fmt          # Format code (3 seconds)
make fmt-check    # Check formatting (3 seconds)  
make clippy       # Run clippy (**NEVER CANCEL**: 10+ minutes)
make check        # Compilation check (5+ minutes)
make test         # Run tests (**NEVER CANCEL**: 15+ minutes)
make pre-commit   # All checks (**NEVER CANCEL**: 20+ minutes)
make setup-hooks  # Setup git hooks (one-time)
```

### Docker Development
- Local development image: `make docker-dev-local` (**NEVER CANCEL**: 20+ minutes)
- Multi-arch production build: `make docker-buildx` (**NEVER CANCEL**: 30+ minutes)
- Quick examples: `./examples/docker-quickstart.sh basic|dev|prod`

## Validation

### Manual Testing Scenarios
- **ALWAYS** run these scenarios after making changes:
  1. **Build and startup test**: Build rustfs binary, start server, verify it listens on port 9000
  2. **Console access test**: Access web console at http://localhost:9000, login with default credentials `rustfsadmin`
  3. **Basic S3 operations**: Create bucket, upload/download file using S3 API or console
  4. **Configuration test**: Verify configuration loading and environment variable handling

### Validation Commands
- Always run `make pre-commit` before submitting changes (**NEVER CANCEL**: 20+ minutes)
- Check for typos: Uses custom `./_typos.toml` config in CI
- One failing test is expected: `rustfs-ecstore endpoints::test::test_create_pool_endpoints` (hostname resolution issue in sandbox)

## Repository Structure

### Key Entry Points
- Main binary: `rustfs/src/main.rs`
- Core modules: `rustfs/src/{admin,auth,config,server,storage,license.rs,profiling.rs}`
- Crate structure: 25+ crates in `/crates/` directory

### Important Crates
- `ecstore` - Erasure coding storage (core storage layer)
- `iam` - Identity and Access Management  
- `madmin` - Management dashboard and admin API
- `s3select-api` & `s3select-query` - S3 Select implementation
- `config` - Configuration management with notify features
- `crypto` - Cryptography and security
- `common` - Shared utilities
- `obs` - Observability and logging

### Build System Files
- `Cargo.toml` - Workspace configuration with 25+ members
- `build-rustfs.sh` - Primary build script with cross-compilation support
- `Makefile` - Build automation (works in sandbox)
- `Justfile` - Alternative task runner (not available in sandbox, use Make instead)
- `.github/workflows/ci.yml` - CI pipeline configuration

### Documentation Files
- `README.md` - Main project documentation
- `CONTRIBUTING.md` - Development workflow and code quality requirements
- `CLAUDE.md` - Project overview for AI assistants
- `examples/README.md` - Docker deployment examples

## Common Issues and Solutions

### Build Issues
- **Disk space**: Builds require significant disk space, clean target/ directory if needed
- **Linker errors**: Use standard `cargo build` instead of `./build-rustfs.sh` in constrained environments
- **Console assets**: Run `touch rustfs/build.rs` to trigger console asset embedding
- **Cross-compilation**: Use `--skip-verification` flag for cross-platform builds

### Testing Issues  
- **One expected failure**: `rustfs-ecstore endpoints::test::test_create_pool_endpoints` due to hostname resolution
- **Timeout issues**: Always use **NEVER CANCEL** and set 30+ minute timeouts for builds, 20+ minutes for tests
- **E2E setup**: Requires s3s-e2e tool installation and proper binary path

### Environment Setup
- **Rust version**: Uses `rust-toolchain.toml` to pin version 
- **Dependencies**: cargo-nextest improves test performance significantly
- **Just command**: Not available in sandbox environments, use Make targets instead

## Performance Notes

### Expected Command Timing
- `cargo fmt --all`: 3 seconds
- `cargo check --all-targets`: 5-6 minutes  
- `cargo build -p rustfs --bins`: 8-12 minutes
- `cargo build --release -p rustfs --bins`: 15+ minutes
- `cargo nextest run --all --exclude e2e_test`: 8-15 minutes
- `cargo test --all --doc`: 2-3 minutes
- `cargo clippy --all-targets --all-features -- -D warnings`: 10+ minutes
- `make pre-commit`: 20+ minutes total
- Docker builds: 20-30+ minutes

### Resource Requirements
- Significant disk space for target/ directory
- High memory usage during parallel compilation
- Network bandwidth for dependency downloads

## Development Workflow

### Standard Process
1. **Format code**: `make fmt`
2. **Quick check**: `cargo check --all-targets` (5+ minutes)
3. **Run tests**: `make test` (**NEVER CANCEL**: 15+ minutes)  
4. **Lint code**: `make clippy` (**NEVER CANCEL**: 10+ minutes)
5. **Manual validation**: Build and test key scenarios
6. **Final check**: `make pre-commit` (**NEVER CANCEL**: 20+ minutes)

### Manual Validation Steps
1. Build the binary: `cargo build -p rustfs --bins`
2. Start server: `./target/debug/rustfs` 
3. Verify startup logs and port binding
4. Test console access at http://localhost:9000
5. Login with credentials: `rustfsadmin` / `rustfsadmin`
6. Create a test bucket and upload/download a file
7. Verify S3 compatibility with basic operations

**CRITICAL REMINDER**: Never cancel long-running builds or tests. Set appropriate timeouts (30+ minutes for builds, 20+ minutes for tests) and let them complete. Build failures due to premature cancellation waste significant time.