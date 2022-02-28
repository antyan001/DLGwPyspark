#!/bin/env python3

import os, sys
dlg = os.environ.get("DLG_CLICKSTREAM")
sys.path.insert(0, dlg)
os.chdir(dlg)

import logging

################################## Logging ##################################


def log(message, logger, 
        print_log : bool = True):
    if not print_log:
        return

    logger.info(message)
    #print(message, file=sys.stdout)
    print(message, file=sys.stderr)


def class_method_logger(func):    
    MAX_ARG_LEN = 120
    LINE_BREAK_LEN = 120

    ######################### Args #########################

    def transform_args_to_str(args):
        args_arr = []
        for arg in args:
            if len(str(arg)) > MAX_ARG_LEN:
                args_arr.append(type(arg))
            else:
                args_arr.append(str(arg))
        return str(args_arr)
    
    def print_args(args_, self_log):
        if len(args_) != 0:
            args_tr = transform_args_to_str(args_)
            new_line = ""
            if len(args_tr) > LINE_BREAK_LEN:
                new_line = "\n"
            self_log("with args: {nl}{args}".format(args=args_tr, nl=new_line))
    
    ######################### Kwargs #########################

    def transform_kwargs_to_str(kwargs):
        kwargs_dict = {}
        for key, value in kwargs.items():
            if len(str(value)) > MAX_ARG_LEN:
                kwargs_dict[key] = type(value)
            else:
                kwargs_dict[key] = value
        return str(kwargs_dict)
    
    def print_kwargs(kwargs, self_log):
        if len(kwargs) != 0:
            kwargs_tr = transform_kwargs_to_str(kwargs)
            new_line = ""
            if len(kwargs_tr) > LINE_BREAK_LEN:
                new_line = "\n"
            self_log("with kwargs: {nl}{kwargs}".format(kwargs=kwargs_tr, nl=new_line))

    ######################### Wrapper ######################### 

    def transform_return_to_str(func_return):
        if len(str(func_return)) > MAX_ARG_LEN:
            return str(type(func_return))
        return str(func_return)
        
    def print_return(func_return, self_log):
        if func_return is not None:
            func_return_rf = transform_return_to_str(func_return)
            new_line = ""
            if len(func_return_rf) > LINE_BREAK_LEN:
                new_line = "\n"
            self_log("return: {nl}{return_val}".format(return_val=func_return_rf, nl=new_line))
            
    ######################### Wrapper ######################### 
    
    def wrapper(*args, **kwargs):
        self_ = args[0]
        self_log = lambda msg : log(msg, self_.logger, self_.print_log) 
        
        # BEGIN
        self_log("# {} : begin".format(func.__name__))

        # ARGS, KWARGS
        args_ = args[1:]
        print_args(args_, self_log)
        print_kwargs(kwargs, self_log)

        # EXCEPTION
        try:
            func_return = func(*args, **kwargs)
        except Exception as ex:
            # self_log("##### Exception in function {func}:".format(func=func.__name__))
            # self_log(str(ex))
            raise

        # END
        self_log("# {} : end".format(func.__name__))

        # RETURN
        print_return(func_return, self_log)

        return func_return

    return wrapper
