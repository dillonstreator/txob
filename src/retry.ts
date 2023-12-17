import retry from 'retry';

export const retryable = <T>(
    action: () => Promise<T>,
    options?: retry.OperationOptions
): Promise<T> => {
    return new Promise((resolve, reject) => {
        const op = retry.operation(options);

        op.attempt(async () => {
            try {
                resolve(await action());
            } catch (err) {
                if (!op.retry(err as Error)) reject(err);
            }
        });
    });
};
