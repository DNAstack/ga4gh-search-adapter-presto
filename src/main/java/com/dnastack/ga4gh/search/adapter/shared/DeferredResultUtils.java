package com.dnastack.ga4gh.search.adapter.shared;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.core.SingleObserver;
import io.reactivex.rxjava3.core.SingleOperator;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.util.concurrent.Callable;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.async.DeferredResult;

public class DeferredResultUtils {


    public static <T> DeferredResult<T> ofSingle(Callable<Single<T>> singleProducer) {
        DeferredResult<T> result = new DeferredResult<>();
        try {
            Single<T> fromCall = singleProducer.call();
            fromCall.subscribeOn(Schedulers.io())
                .subscribe(result::setResult, result::setErrorResult);
        } catch (Exception e) {
            result.setErrorResult(e);
        }

        return result;
    }

    public static <T> DeferredResult<T> ofObservable(Callable<Observable<T>> observableCallable) {
        DeferredResult<T> result = new DeferredResult<>();
        try {
            Observable<T> fromCall = observableCallable.call();
            fromCall.subscribeOn(Schedulers.io()).subscribe(result::setResult, result::setErrorResult);
        } catch (Exception e) {
            result.setErrorResult(e);
        }
        return result;
    }
}
