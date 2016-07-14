extern crate pqbus;

#[test]
fn test_connect() {
    let bus = pqbus::new("bla", "work");
    assert!(bus.is_err());
}
