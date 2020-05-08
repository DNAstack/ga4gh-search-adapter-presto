package com.dnastack.ga4gh.search.adapter.shared;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import java.util.concurrent.Callable;
import org.springframework.web.context.request.async.DeferredResult;

public class DeferredResultUtils {


    public static <T> DeferredResult<T> ofSingle(Callable<Single<T>> singleProducer){
        DeferredResult<T> result = new DeferredResult<>();
        try {
            Single<T> fromCall = singleProducer.call();
            fromCall.subscribe(result::setResult,result::setErrorResult);
        } catch (Exception e){
            result.setErrorResult(e);
        }
        return result;
    }

    public static <T> DeferredResult<T> ofObservable(Callable<Observable<T>> observableCallable){
        DeferredResult<T> result = new DeferredResult<>();
        try {
            Observable<T> fromCall = observableCallable.call();
            fromCall.subscribe(result::setResult,result::setErrorResult);
        } catch (Exception e){
            result.setErrorResult(e);
        }
        return result;
    }


}
