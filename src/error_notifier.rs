/*use std::sync::Arc;

pub trait ErrorNotifier: Clone {
    type Error: std::error::Error;

    fn notify(&self, error: Self::Error);
}

#[derive(Clone)]
pub struct TracingErrorNotifier<E: std::error::Error> {
    name: Arc<str>,
    phantom: std::marker::PhantomData<E>,
}

impl<E: std::error::Error> TracingErrorNotifier<E> {
    pub fn new(name: impl Into<Arc<str>>) -> Self {
        Self {
            name: name.into(),
            phantom: std::marker::PhantomData,
        }
    }
}

impl<E: std::error::Error + Clone> ErrorNotifier for TracingErrorNotifier<E> {
    type Error = E;

    fn notify(&self, error: Self::Error) {
        tracing::error!(error=%error, name=%self.name,
            "ErrorNotifier reported");
    }
}
*/
// #[test]
// fn test_error_notifier() {
//     tracing_subscriber::fmt::init();

//     let notifier =
//         TracingErrorNotifier::<std::io::Error>::new("some module name");
//     notifier
//         .notify(std::io::Error::new(std::io::ErrorKind::Other, "Test error"));
// }
