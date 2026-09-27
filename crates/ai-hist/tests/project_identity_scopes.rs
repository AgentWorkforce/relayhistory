//! The configuration git reads from outside the repository.
//!
//! Its own test binary, and one test function, because it sets `HOME` and the
//! `GIT_CONFIG_*` variables for the whole process — the same reason git itself
//! offers those variables: there is no other way to ask "what would git see
//! here" without answering it from the machine the test happens to run on.
//!
//! The claim under test is not that these files parse. It is that a remote
//! only a *global* rewrite can expand resolves to the same canonical key as
//! one written out in full. A reader that stops at `.git/config` sees
//! `gh:Org/Repo.git` as junk and falls back to a path key, so every session on
//! that machine fragments — and the machines that configure rewrites are the
//! ones whose owners configured git most carefully.

use ai_hist::project_identity::{project_identity, ProjectKeyMethod};
use std::fs;
use std::path::Path;

fn repo_with_origin(root: &Path, name: &str, origin: &str) -> std::path::PathBuf {
    let dir = root.join(name);
    let git = dir.join(".git");
    fs::create_dir_all(&git).unwrap();
    fs::write(
        git.join("config"),
        format!("[remote \"origin\"]\n\turl = {origin}\n"),
    )
    .unwrap();
    dir
}

#[test]
fn global_and_conditional_git_config_resolve_a_rewritten_remote() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path();
    std::env::set_var("HOME", home);
    std::env::set_var("USERPROFILE", home);
    std::env::remove_var("XDG_CONFIG_HOME");
    std::env::remove_var("GIT_CONFIG_GLOBAL");
    // Whatever this machine has in /etc is not part of the claim.
    std::env::set_var("GIT_CONFIG_NOSYSTEM", "1");

    let shorthand = repo_with_origin(&home.join("work"), "app", "gh:Org/Repo.git");

    // --- the control: no rewrite is configured anywhere -------------------
    let unresolved = project_identity(&shorthand);
    assert_eq!(
        unresolved.method,
        ProjectKeyMethod::PathFallback,
        "`gh:Org/Repo.git` is not a URL any rule can canonicalize on its own"
    );

    // --- ~/.gitconfig ----------------------------------------------------
    fs::write(
        home.join(".gitconfig"),
        "[url \"git@github.com:\"]\n\tinsteadOf = gh:\n",
    )
    .unwrap();
    let resolved = project_identity(&shorthand);
    assert_eq!(
        (resolved.project_key.as_str(), resolved.method),
        ("github.com/Org/Repo", ProjectKeyMethod::Remote),
        "a rewrite in the global scope was not applied"
    );

    // --- includeIf "gitdir:", pulled in from the global scope -------------
    fs::write(
        home.join(".gitconfig"),
        format!(
            "[url \"git@github.com:\"]\n\tinsteadOf = gh:\n\
             [includeIf \"gitdir:{home}/acme/\"]\n\tpath = {home}/acme-identity\n",
            home = home.display()
        ),
    )
    .unwrap();
    fs::write(
        home.join("acme-identity"),
        "[url \"git@github.com:acme/\"]\n\tinsteadOf = acme:\n",
    )
    .unwrap();
    let inside = repo_with_origin(&home.join("acme"), "thing", "acme:thing.git");
    let outside = repo_with_origin(&home.join("other"), "thing", "acme:thing.git");
    assert_eq!(
        (
            project_identity(&inside).project_key.as_str(),
            project_identity(&inside).method
        ),
        ("github.com/acme/thing", ProjectKeyMethod::Remote),
        "a conditional include whose gitdir condition holds was not applied"
    );
    assert_eq!(
        project_identity(&outside).method,
        ProjectKeyMethod::PathFallback,
        "a conditional include must not apply where its condition does not hold"
    );
    // The unconditional rewrite in the same file still applies everywhere.
    assert_eq!(
        project_identity(&shorthand).project_key,
        "github.com/Org/Repo"
    );

    // --- GIT_CONFIG_GLOBAL replaces the home file, as it does for git -----
    fs::write(
        home.join("elsewhere.gitconfig"),
        "[url \"https://gitlab.com/\"]\n\tinsteadOf = gh:\n",
    )
    .unwrap();
    std::env::set_var("GIT_CONFIG_GLOBAL", home.join("elsewhere.gitconfig"));
    assert_eq!(
        project_identity(&shorthand).project_key,
        "gitlab.com/Org/Repo",
        "GIT_CONFIG_GLOBAL must be read instead of ~/.gitconfig, not beside it"
    );
    assert_eq!(
        project_identity(&inside).method,
        ProjectKeyMethod::PathFallback,
        "the replaced file's conditional include must be gone with it"
    );

    // --- both default global files are read, not one of them --------------
    //
    // Checked against git 2.43 in this environment, not taken from the
    // documentation, because the obvious reading is the wrong one:
    // `git config --global --list` shows only `~/.gitconfig`, which makes it
    // look as though the XDG file is ignored once that exists. It is not —
    // that is the *write* target. Both files are read, in XDG-then-home
    // order:
    //
    //   ~/.config/git/config  [url "https://xdg.example/"] insteadOf = gh:
    //   ~/.gitconfig          [user] name = Someone
    //   $ git remote get-url origin   → https://xdg.example/Org/Repo.git
    //
    // Reading only `~/.gitconfig` when it exists would drop a rewrite git
    // applies, which is the same class of defect as not reading the global
    // scopes at all — and it would hit hardest on machines that keep their
    // git configuration in XDG and have a `~/.gitconfig` holding nothing but
    // a name and an email.
    // The section above left an explicit global file in place, and these
    // assertions are about the *defaults*.
    std::env::remove_var("GIT_CONFIG_GLOBAL");
    let xdg_config = home.join(".config/git/config");
    fs::create_dir_all(xdg_config.parent().unwrap()).unwrap();
    fs::write(
        &xdg_config,
        "[url \"https://xdg.example/\"]\n\tinsteadOf = xdg:\n",
    )
    .unwrap();
    fs::write(home.join(".gitconfig"), "[user]\n\tname = Someone\n").unwrap();
    let xdg_only = repo_with_origin(&home.join("xdg-only"), "thing", "xdg:acme/thing.git");
    assert_eq!(
        project_identity(&xdg_only).project_key,
        "xdg.example/acme/thing",
        "the XDG global file must still be read when ~/.gitconfig exists"
    );

    // Both files contribute, and where they rewrite the same prefix the one
    // git reads first wins — again as git answers it, not as it reads.
    fs::write(
        home.join(".gitconfig"),
        "[user]\n\tname = Someone\n[url \"https://home.example/\"]\n\tinsteadOf = hm:\n",
    )
    .unwrap();
    let home_scoped = repo_with_origin(&home.join("home-scoped"), "thing", "hm:acme/thing.git");
    assert_eq!(
        project_identity(&home_scoped).project_key,
        "home.example/acme/thing",
        "~/.gitconfig must be read beside the XDG file, not instead of it"
    );
    assert_eq!(
        project_identity(&xdg_only).project_key,
        "xdg.example/acme/thing",
        "and reading one must not stop the other from applying"
    );

    // `GIT_CONFIG_GLOBAL` replaces *both* of them, which is the one case where
    // a default global file is genuinely ignored.
    fs::write(
        home.join("explicit.gitconfig"),
        "[url \"https://explicit.example/\"]\n\tinsteadOf = xdg:\n",
    )
    .unwrap();
    std::env::set_var("GIT_CONFIG_GLOBAL", home.join("explicit.gitconfig"));
    assert_eq!(
        project_identity(&xdg_only).project_key,
        "explicit.example/acme/thing",
        "an explicit global file must replace the XDG one as well as the home one"
    );
    assert_eq!(
        project_identity(&home_scoped).method,
        ProjectKeyMethod::PathFallback,
        "and the home file's rewrites must go with it"
    );
    std::env::remove_var("GIT_CONFIG_GLOBAL");
    fs::remove_file(&xdg_config).unwrap();

    // --- a remote's URLs accumulate across scopes, head first -------------
    //
    // This is what git does, checked rather than assumed: with
    // `remote.origin.url` set in both scopes, git 2.43 answers
    // `git remote get-url origin` with the *global* one, because the two are
    // one list read in scope order and `get-url` prints its head. An earlier
    // version of this test asserted the opposite from intuition; the
    // intuition was wrong, and a reader that matched it would disagree with
    // the command it replaced.
    std::env::remove_var("GIT_CONFIG_GLOBAL");
    fs::write(
        home.join(".gitconfig"),
        "[url \"git@github.com:\"]\n\tinsteadOf = gh:\n\
         [remote \"origin\"]\n\turl = gh:Global/First.git\n",
    )
    .unwrap();
    assert_eq!(
        project_identity(&shorthand).project_key,
        "github.com/Global/First",
        "a remote's URL list is read in scope order and its head is the remote"
    );
}
